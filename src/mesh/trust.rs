//! Membership: signed admission and removal records, and the member set they imply.
//!
//! Every member keeps the records it takes, and passes them on (announcements, and a full
//! exchange whenever two members find their sets differ, unless an exchange at the same two sets
//! changed neither). A record is taken if its signature is good, some record admits its signer,
//! and it isn't void (below) in the view of the records held and those arriving with it, but for
//! this machine's own records and those admitting it; a void record is turned away only when the
//! view without it is the same (see [`Trust::merge_checked`]). A record that counts nowhere but
//! isn't void is kept and passed on: beside records another member holds it may count, or
//! decide what else counts, and a member that hears from another only through a third gets it
//! only from the third. A record held that turns void stays until the store is next loaded (a
//! daemon start; see [`Trust::prune`]). So during a run a member's records only grow, and two
//! members that exchange them until neither changes hold sets with the same view: members agree
//! on who is in the cluster whenever each reaches the others, directly or through others. There
//! is no cap on records: those of removed machines (their admission, their removal and the
//! records it keeps) count, and are kept for good. The member set is a function of the record
//! set alone. No wall-clock time decides anything: `at` is signed but only shown.
//!
//! - The cluster's root is its founder's self-signed admission; the cluster id is the founder's
//!   key.
//! - Each record has an id (the SHA-256 of what was signed) and a logical `clock`: one more than
//!   the highest clock among the valid records its signer knew. Along a chain of admissions the
//!   clock rises, so it orders admissions causally.
//! - **An admission counts** if its signer's own admission counts (or the signer is the founder)
//!   with a lower clock, nothing voids it (below), its name is free: no earlier admission under
//!   that name for another key counts, unless that key's removal counts and came first (a lower
//!   clock), and there is room: fewer than [`MAX_MEMBERS`] other keys have an earlier admission
//!   that counts and no removal that counts and came first. So the cluster has at most
//!   [`MAX_MEMBERS`] members, and of two admissions that would each be the last to fit (two
//!   members admitting at once), the earlier (by clock, then id) counts on every member.
//! - **A removal** of key K lists the ids of the records signed by K that counted where the
//!   remover was when it signed (and those admitting the remover). If the removal counts, K is
//!   refused for good, and every record signed by K that is not on the list is void, whatever its
//!   date, with everything that depends on it: the records of keys no record left admits. So a
//!   removed machine can't sign its way back in, admit anyone, squat a name or remove anyone,
//!   whether it dates its records back (an early `at` or `clock` makes no difference: what the
//!   remover hadn't seen is void) or held them back, and a member that was offline when it was
//!   removed may pass its records on but they are void, so they are turned away, and dropped
//!   where held when the store is next loaded. The one way back is a removal of the remover by K ranked before it (below):
//!   the rank comes from K's admission, which K didn't sign. The records K made that the remover
//!   knew of stay valid, the machines K admitted included. A record of K's the remover hadn't
//!   seen is lost, even an honest one: a machine K admitted moments before its removal may have
//!   to be admitted again.
//! - A removal counts if its signer's admission counts and no removal that counts voids it.
//!   **Removals that void each other** (A removes B while B removes A, neither knowing of the
//!   other's, or longer cycles) are settled by rank: the removal signed by the member admitted
//!   earliest (the founder first, then by the clock of its admission, which the member that
//!   admitted it signed) counts, and the removals in conflict with it are void. So a member can
//!   be removed against its will only by a member ranked before it; the founder can't be.
//! - **What a store holds**, then, besides this machine's own records and those admitting it: the
//!   records of keys admitted by records that aren't void, but those a removal of their key
//!   voids. A member can make every member hold what it signs while it is a member, and what the
//!   keys it admitted sign meanwhile, whether their admission counts or not; once it is removed,
//!   only its records that counted where its remover was stay, and what the other keys it
//!   admitted sign is void. The records of a key no record admits are never held, nor those a
//!   removed key signs after its removal, but by a member that took them before it heard of the
//!   removal: it holds them, void, until it is next loaded, and the others, which get them with
//!   the removal, turn them away.
//!
//! **Users.** The above is a cluster without users, where members' machine keys sign admissions
//! and removals (v2 records). A user's key, held by a command line ([`super::user`]), signs
//! records of its own; they carry other tags in their signed bytes, so v2 records keep theirs and
//! their ids. See [`evaluate`] for what counts:
//!
//! - The first user's admission (the genesis) is signed by its own key and co-signed by a
//!   member's machine key. Once one counts, users sign what counts: a user's admission of a
//!   machine (v3, with its pool) or removal of one, and a user's admission or removal of a user.
//!   What members sign counts for nothing from then on; until then, what users sign does.
//! - Users form a chain of trust from the genesis as members do from the founder: a user's
//!   admission counts if its signer is a user, a removal lists the records of the removed user it
//!   keeps, removals that void each other go to the user admitted first.
//! - Of competing geneses, the one co-signed by the member admitted earliest counts. Members are
//!   then taken as the v2 records say, without the removals the removed machine could undo, so a
//!   machine admitted after the co-signer can't unseat the users; one admitted before it can,
//!   with a genesis of its own, as it could remove the co-signer before users.
//! - A machine signs nothing that counts once there are users, so a user's removal of a machine
//!   keeps nothing and is final.

use super::MeshError;
use super::identity::{Identity, PublicKey, Signature, Signer, write_private};
use crate::types::{Millis, NodeName};
use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD as B64;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet, VecDeque};
use std::fmt;
use std::path::{Path, PathBuf};

/// The trust records file in the keys directory.
pub const STATE_FILE: &str = "members.json";

/// At most this many members: an admission past it counts nowhere (see the module docs), and
/// [`Mesh::admit`](super::Mesh::admit) refuses to sign one.
pub const MAX_MEMBERS: usize = 2048;

const REQUEST_PREFIX: &str = "potluck-pair-request.";
const REPLY_PREFIX: &str = "potluck-pair-reply.";

/// A record's id: the SHA-256 of what its signer signed. Written as unpadded base64url.
#[derive(Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct RecordId(pub [u8; 32]);

impl RecordId {
    fn of(signed: &[u8]) -> RecordId {
        let d = ring::digest::digest(&ring::digest::SHA256, signed);
        let mut out = [0u8; 32];
        out.copy_from_slice(d.as_ref());
        RecordId(out)
    }
}

impl fmt::Debug for RecordId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "RecordId({})", B64.encode(self.0))
    }
}

impl Serialize for RecordId {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        s.serialize_str(&B64.encode(self.0))
    }
}

impl<'de> Deserialize<'de> for RecordId {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let s = String::deserialize(d)?;
        let bytes = B64
            .decode(s.trim())
            .map_err(|_| serde::de::Error::custom("bad record id"))?;
        let arr: [u8; 32] = bytes
            .try_into()
            .map_err(|_| serde::de::Error::custom("bad record id length"))?;
        Ok(RecordId(arr))
    }
}

/// The pool of a machine whose admission names none: one a member signed (v2), and the default of
/// `potluck admit`.
pub const DEFAULT_POOL: &str = "default";

/// Whether `pool` is usable as a pool's name: the characters of a node name.
pub fn valid_pool(pool: &str) -> bool {
    valid_name(pool)
}

fn is_false(b: &bool) -> bool {
    !*b
}

/// A signed statement that a machine joins the cluster: by a member's machine key (v2, `pool`
/// None), or by a user's key (v3, `pool` set; `by` is then the user's name).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Admission {
    pub node: NodeName,
    pub key: PublicKey,
    /// Where it said it can be reached, `host:port`, when it asked to join.
    pub address: String,
    pub by: NodeName,
    pub by_key: PublicKey,
    /// When it was signed, by the signer's clock. Shown only; nothing depends on it.
    pub at: Millis,
    /// Logical time: one more than the highest clock among the valid records the signer knew.
    pub clock: u64,
    /// From the pairing request, so the new machine can tell the reply is for it.
    pub nonce: String,
    /// The machine's pool, in an admission a user signed (v3). Absent from a member's (v2), whose
    /// records keep their bytes and ids.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pool: Option<String>,
    pub sig: Signature,
}

impl Admission {
    fn signed_bytes(&self) -> Vec<u8> {
        match &self.pool {
            None => serde_json::to_vec(&(
                "potluck admission v2",
                &self.node,
                &self.key,
                &self.address,
                &self.by,
                &self.by_key,
                self.at,
                self.clock,
                &self.nonce,
            )),
            Some(pool) => serde_json::to_vec(&(
                "potluck admission v3",
                &self.node,
                &self.key,
                &self.address,
                pool,
                &self.by,
                &self.by_key,
                self.at,
                self.clock,
                &self.nonce,
            )),
        }
        .expect("serializable")
    }

    /// Whether a user signed it (v3), not a member's machine key (v2).
    pub fn by_user(&self) -> bool {
        self.pool.is_some()
    }

    /// The machine's pool: [`DEFAULT_POOL`] for an admission a member signed.
    pub fn pool_name(&self) -> &str {
        self.pool.as_deref().unwrap_or(DEFAULT_POOL)
    }

    /// Signs, with user key `user` (named `by`), an admission of `key` as `node` into `pool`, at
    /// logical time `clock` (see [`Trust::next_clock`]).
    #[allow(clippy::too_many_arguments)]
    pub fn sign_by_user(
        user: &dyn Signer,
        by: &str,
        node: &str,
        key: PublicKey,
        address: &str,
        pool: &str,
        nonce: &str,
        clock: u64,
    ) -> Admission {
        let mut a = Admission {
            node: node.to_string(),
            key,
            address: address.to_string(),
            by: by.to_string(),
            by_key: user.public(),
            at: crate::types::now_ms(),
            clock,
            nonce: nonce.to_string(),
            pool: Some(pool.to_string()),
            sig: Signature([0; 64]),
        };
        a.sig = user.sign(&a.signed_bytes());
        a
    }

    pub fn verify(&self) -> bool {
        self.by_key.verify(&self.signed_bytes(), &self.sig)
    }

    pub fn id(&self) -> RecordId {
        RecordId::of(&self.signed_bytes())
    }

    /// Signs an admission of `key` as `node`, at logical time `clock` (see
    /// [`Trust::next_clock`]).
    #[allow(clippy::too_many_arguments)]
    pub fn sign(
        id: &Identity,
        by: &str,
        node: &str,
        key: PublicKey,
        address: &str,
        nonce: &str,
        clock: u64,
    ) -> Admission {
        let mut a = Admission {
            node: node.to_string(),
            key,
            address: address.to_string(),
            by: by.to_string(),
            by_key: id.public(),
            at: crate::types::now_ms(),
            clock,
            nonce: nonce.to_string(),
            pool: None,
            sig: Signature([0; 64]),
        };
        a.sig = id.sign(&a.signed_bytes());
        a
    }

    /// Whether this is a cluster's founding record (signed by the key it admits).
    pub fn is_founding(&self) -> bool {
        self.by_key == self.key
    }
}

/// A signed statement that a machine is out of the cluster for good: by a member's machine key
/// (v2), or by a user's key (v3, `by_user`; `by` is then the user's name).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Removal {
    pub node: NodeName,
    pub key: PublicKey,
    pub by: NodeName,
    pub by_key: PublicKey,
    /// When it was signed, by the signer's clock. Shown only; nothing depends on it.
    pub at: Millis,
    /// Logical time, as for [`Admission::clock`].
    pub clock: u64,
    /// The ids of every record signed by `key` that the remover held when it signed: these stay
    /// valid, any other record `key` signed is void (see the module docs). Empty in a user's
    /// removal: a machine signs nothing that counts once there are users.
    pub kept: Vec<RecordId>,
    /// Signed by a user (v3). Absent from a member's (v2), whose records keep their bytes and ids.
    #[serde(default, skip_serializing_if = "is_false")]
    pub by_user: bool,
    pub sig: Signature,
}

impl Removal {
    fn signed_bytes(&self) -> Vec<u8> {
        let tag = match self.by_user {
            false => "potluck removal v2",
            true => "potluck removal v3",
        };
        serde_json::to_vec(&(
            tag,
            &self.node,
            &self.key,
            &self.by,
            &self.by_key,
            self.at,
            self.clock,
            &self.kept,
        ))
        .expect("serializable")
    }

    /// Signs, with user key `user` (named `by`), a removal of machine `key` (named `node`), at
    /// logical time `clock`.
    pub fn sign_by_user(
        user: &dyn Signer,
        by: &str,
        node: &str,
        key: PublicKey,
        clock: u64,
    ) -> Removal {
        let mut r = Removal {
            node: node.to_string(),
            key,
            by: by.to_string(),
            by_key: user.public(),
            at: crate::types::now_ms(),
            clock,
            kept: Vec::new(),
            by_user: true,
            sig: Signature([0; 64]),
        };
        r.sig = user.sign(&r.signed_bytes());
        r
    }

    pub fn verify(&self) -> bool {
        self.by_key.verify(&self.signed_bytes(), &self.sig)
    }

    pub fn id(&self) -> RecordId {
        RecordId::of(&self.signed_bytes())
    }

    /// Signs a removal of `key` (named `node`), keeping the records of its listed in `kept`, at
    /// logical time `clock`.
    pub fn sign(
        id: &Identity,
        by: &str,
        node: &str,
        key: PublicKey,
        kept: Vec<RecordId>,
        clock: u64,
    ) -> Removal {
        let mut r = Removal {
            node: node.to_string(),
            key,
            by: by.to_string(),
            by_key: id.public(),
            at: crate::types::now_ms(),
            clock,
            kept,
            by_user: false,
            sig: Signature([0; 64]),
        };
        r.sig = id.sign(&r.signed_bytes());
        r
    }
}

/// The machine key that co-signs the first user's admission: a member's, when it was signed.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CoSign {
    pub node: NodeName,
    pub key: PublicKey,
    pub sig: Signature,
}

/// A user's signed statement that a key is a user's: a key that signs who joins the cluster and
/// who leaves, and who else is a user. The first user's (the genesis) is signed by its own key and
/// co-signed by a member's machine key (`machine`); every other one by a user.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct UserAdmission {
    pub name: String,
    pub key: PublicKey,
    pub by: String,
    pub by_key: PublicKey,
    /// When it was signed, by the signer's clock. Shown only; nothing depends on it.
    pub at: Millis,
    /// Logical time, as for [`Admission::clock`].
    pub clock: u64,
    pub nonce: String,
    /// The genesis only: the member whose machine key co-signed it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub machine: Option<CoSign>,
    pub sig: Signature,
}

impl UserAdmission {
    /// What the user signs, and for the genesis the machine too.
    pub fn signed_bytes(&self) -> Vec<u8> {
        serde_json::to_vec(&(
            "potluck user v1",
            &self.name,
            &self.key,
            &self.by,
            &self.by_key,
            self.at,
            self.clock,
            &self.nonce,
            self.machine.as_ref().map(|m| (&m.node, &m.key)),
        ))
        .expect("serializable")
    }

    /// The user's signature is good, and for the genesis (which only the key it admits signs)
    /// the machine's too.
    pub fn verify(&self) -> bool {
        let bytes = self.signed_bytes();
        self.by_key.verify(&bytes, &self.sig)
            && self
                .machine
                .as_ref()
                .is_none_or(|m| self.by_key == self.key && m.key.verify(&bytes, &m.sig))
    }

    pub fn id(&self) -> RecordId {
        RecordId::of(&self.signed_bytes())
    }

    /// Whether this is a first user's admission: signed by the key it admits, co-signed by a
    /// machine.
    pub fn is_genesis(&self) -> bool {
        self.machine.is_some() && self.by_key == self.key
    }

    /// Signs, with user key `user` (named `by`), an admission of user `key` as `name`.
    pub fn sign(user: &dyn Signer, by: &str, name: &str, key: PublicKey, clock: u64) -> Self {
        let mut u = UserAdmission {
            name: name.to_string(),
            key,
            by: by.to_string(),
            by_key: user.public(),
            at: crate::types::now_ms(),
            clock,
            nonce: nonce(),
            machine: None,
            sig: Signature([0; 64]),
        };
        u.sig = user.sign(&u.signed_bytes());
        u
    }

    /// A first user's admission of `user`'s own key as `name`, signed by it, for the machine
    /// `node` (key `machine`) to co-sign ([`UserAdmission::co_sign`]).
    pub fn genesis(
        user: &dyn Signer,
        name: &str,
        node: &str,
        machine: PublicKey,
        clock: u64,
    ) -> Self {
        let mut u = UserAdmission {
            name: name.to_string(),
            key: user.public(),
            by: name.to_string(),
            by_key: user.public(),
            at: crate::types::now_ms(),
            clock,
            nonce: nonce(),
            machine: Some(CoSign {
                node: node.to_string(),
                key: machine,
                sig: Signature([0; 64]),
            }),
            sig: Signature([0; 64]),
        };
        u.sig = user.sign(&u.signed_bytes());
        u
    }

    /// Co-signs a genesis with machine key `id`, which it names.
    pub fn co_sign(&mut self, id: &Identity) -> Result<(), String> {
        let bytes = self.signed_bytes();
        let Some(m) = &mut self.machine else {
            return Err("not a first user's admission".into());
        };
        if m.key != id.public() {
            return Err("it names another machine's key".into());
        }
        m.sig = id.sign(&bytes);
        Ok(())
    }
}

/// A user's signed statement that a key is no user's any more.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct UserRemoval {
    pub name: String,
    pub key: PublicKey,
    pub by: String,
    pub by_key: PublicKey,
    pub at: Millis,
    pub clock: u64,
    /// As for [`Removal::kept`]: the records `key` signed that count where it was signed stay
    /// valid; any other record it signed is void.
    pub kept: Vec<RecordId>,
    pub sig: Signature,
}

impl UserRemoval {
    fn signed_bytes(&self) -> Vec<u8> {
        serde_json::to_vec(&(
            "potluck user removal v1",
            &self.name,
            &self.key,
            &self.by,
            &self.by_key,
            self.at,
            self.clock,
            &self.kept,
        ))
        .expect("serializable")
    }

    pub fn verify(&self) -> bool {
        self.by_key.verify(&self.signed_bytes(), &self.sig)
    }

    pub fn id(&self) -> RecordId {
        RecordId::of(&self.signed_bytes())
    }

    /// Signs, with user key `user` (named `by`), a removal of user `key` (named `name`) keeping
    /// the records of its listed in `kept`.
    pub fn sign(
        user: &dyn Signer,
        by: &str,
        name: &str,
        key: PublicKey,
        kept: Vec<RecordId>,
        clock: u64,
    ) -> UserRemoval {
        let mut r = UserRemoval {
            name: name.to_string(),
            key,
            by: by.to_string(),
            by_key: user.public(),
            at: crate::types::now_ms(),
            clock,
            kept,
            sig: Signature([0; 64]),
        };
        r.sig = user.sign(&r.signed_bytes());
        r
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Record {
    /// A machine's admission: v2 by a member, v3 by a user ([`Admission::by_user`]).
    Admission(Admission),
    /// A machine's removal: v2 by a member, v3 by a user ([`Removal::by_user`]).
    Removal(Removal),
    /// A user's admission.
    User(UserAdmission),
    /// A user's removal.
    UserRemoval(UserRemoval),
}

/// Whose admission a record's signer needs, and what an admission admits: a machine's key as a
/// member (by a v2 admission, or the founder), or a key as a user's.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
enum Tier {
    Machine,
    User,
}

impl Record {
    /// When it was signed (display only).
    pub fn at(&self) -> Millis {
        match self {
            Record::Admission(a) => a.at,
            Record::Removal(r) => r.at,
            Record::User(u) => u.at,
            Record::UserRemoval(r) => r.at,
        }
    }
    pub fn clock(&self) -> u64 {
        match self {
            Record::Admission(a) => a.clock,
            Record::Removal(r) => r.clock,
            Record::User(u) => u.clock,
            Record::UserRemoval(r) => r.clock,
        }
    }
    pub fn sig(&self) -> &Signature {
        match self {
            Record::Admission(a) => &a.sig,
            Record::Removal(r) => &r.sig,
            Record::User(u) => &u.sig,
            Record::UserRemoval(r) => &r.sig,
        }
    }
    /// The key that signed it: a machine's for a v2 record, a user's for the others (the
    /// genesis's own key; its co-signer is [`CoSign::key`]).
    pub fn signer(&self) -> PublicKey {
        match self {
            Record::Admission(a) => a.by_key,
            Record::Removal(r) => r.by_key,
            Record::User(u) => u.by_key,
            Record::UserRemoval(r) => r.by_key,
        }
    }
    pub fn id(&self) -> RecordId {
        match self {
            Record::Admission(a) => a.id(),
            Record::Removal(r) => r.id(),
            Record::User(u) => u.id(),
            Record::UserRemoval(r) => r.id(),
        }
    }
    pub fn verify(&self) -> bool {
        match self {
            Record::Admission(a) => a.verify(),
            Record::Removal(r) => r.verify(),
            Record::User(u) => u.verify(),
            Record::UserRemoval(r) => r.verify(),
        }
    }

    /// A record a member's machine key signed (v2): what counts in a cluster without users.
    pub fn is_legacy(&self) -> bool {
        match self {
            Record::Admission(a) => !a.by_user(),
            Record::Removal(r) => !r.by_user,
            Record::User(_) | Record::UserRemoval(_) => false,
        }
    }

    /// The admission its signer needs held for it to be taken: the genesis's co-signer as a
    /// member, a v2 record's signer as a member, any other's signer as a user.
    fn needs(&self) -> (Tier, PublicKey) {
        match self {
            Record::User(u) if u.is_genesis() => (
                Tier::Machine,
                u.machine.as_ref().map(|m| m.key).unwrap_or(u.by_key),
            ),
            r if r.is_legacy() => (Tier::Machine, r.signer()),
            r => (Tier::User, r.signer()),
        }
    }

    /// Who it admits, if it admits a key that may sign: a v2 admission a member, a user's
    /// admission a user. A user's admission of a machine (v3) admits no signer: machines sign
    /// nothing that counts once there are users.
    fn admits(&self) -> Option<(Tier, PublicKey)> {
        match self {
            Record::Admission(a) if !a.by_user() => Some((Tier::Machine, a.key)),
            Record::User(u) => Some((Tier::User, u.key)),
            _ => None,
        }
    }
}

/// The member set a record set implies.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct View {
    /// Each member's key and the admission that let it in.
    pub members: BTreeMap<PublicKey, Admission>,
    /// Removed keys and their removals.
    pub removed: BTreeMap<PublicKey, Removal>,
    /// The records that count.
    pub valid: BTreeSet<RecordId>,
    /// Every key a held record admits, whether that record counts or not (a [`Trust`] holds
    /// void records only if they are this machine's own or admit it, or turned void after it
    /// took them).
    pub known: BTreeSet<PublicKey>,
    /// The users and the admissions that made them users, once a first user's admission counts
    /// ([`View::genesis`]); none before.
    pub users: BTreeMap<PublicKey, UserAdmission>,
    /// Keys that are no users' any more, and their removals.
    pub users_removed: BTreeMap<PublicKey, UserRemoval>,
    /// The first user's admission that counts. From it on only what users sign makes members.
    pub genesis: Option<RecordId>,
}

impl View {
    /// The member set `records` imply for `cluster` (see the module docs).
    pub fn compute(cluster: &PublicKey, records: &[Record]) -> View {
        evaluate(cluster, records, true, MAX_MEMBERS).view
    }

    /// [`View::compute`] for records whose signatures were checked already (a [`Trust`]'s), with
    /// room for `limit` members ([`MAX_MEMBERS`]; tests use fewer).
    fn of_verified(cluster: &PublicKey, records: &[Record], limit: usize) -> View {
        evaluate(cluster, records, false, limit).view
    }

    pub fn by_name(&self, name: &str) -> Option<&Admission> {
        self.members.values().find(|a| a.node == name)
    }

    /// Whether users sign what counts: a first user's admission counts.
    pub fn users_exist(&self) -> bool {
        self.genesis.is_some()
    }

    /// Everything a view says but [`View::known`]: what a store must not change when it drops
    /// records.
    fn standing(&self) -> impl PartialEq + std::fmt::Debug + '_ {
        (
            &self.members,
            &self.removed,
            &self.valid,
            &self.users,
            &self.users_removed,
            &self.genesis,
        )
    }
}

/// What [`evaluate`] found, beside the view: what [`Trust::why_not_member`] explains with.
struct Evaluated {
    view: View,
    /// For a cluster with users: each user key's lowest admission clock that counts, the
    /// removals of users that count (what each keeps), and the machines' admissions turned away
    /// by name or room.
    tier: Option<MachineTier>,
}

#[derive(Default)]
struct MachineTier {
    user_reach: HashMap<PublicKey, u64>,
    user_kept: HashMap<PublicKey, Vec<HashSet<RecordId>>>,
    name_taken: HashSet<RecordId>,
    no_room: HashSet<RecordId>,
}

impl MachineTier {
    /// Whether a record (id `id`, at `clock`) signed by user key `by` counts as far as its
    /// signer goes: the key is a user's (for an admission, from before `clock`), and no removal
    /// of that user that counts voids it.
    fn signed_ok(&self, id: RecordId, by: &PublicKey, clock: u64, admission: bool) -> bool {
        self.user_reach
            .get(by)
            .is_some_and(|&c| !admission || clock > c)
            && self
                .user_kept
                .get(by)
                .is_none_or(|ks| ks.iter().all(|k| k.contains(&id)))
    }
}

/// The view of `records` (with `verify`, those whose signature is bad left out first):
///
/// - Records members signed (v2) are labelled as before users ([`Eval`]). Without a first user's
///   admission that counts, they alone decide (legacy), and what users signed counts for nothing.
/// - A first user's admission (the genesis) counts if a member's machine key co-signed it, after
///   that member's admission. Members here are as the v2 records say without the removals that
///   their machine could undo (signed by a machine admitted after it): a machine admitted later
///   can't keep the genesis from counting. Of several, the one co-signed by the machine admitted
///   earliest counts (founder first, then by the clock and id of its first admission that
///   counts), then the lower clock, then id; the others count for nothing.
/// - From the genesis on, users are labelled as members were ([`Eval`], the genesis's key as
///   founder): a user's admission counts if its signer is a user from a lower clock; a removal
///   lists what it keeps of the removed user's records, and removals that void each other go to
///   the user admitted first.
/// - Then machines: a user's admission (v3) or removal of a machine counts if its signer is a user
///   (an admission: from a lower clock) and no removal of that user that counts voids it. The
///   members are the keys such admissions admit that no such removal removes, names unique and
///   room for `limit` as before users. Records members signed count for nothing any more.
fn evaluate(cluster: &PublicKey, records: &[Record], verify: bool, limit: usize) -> Evaluated {
    let mut seen = HashSet::new();
    let items: Vec<(RecordId, &Record)> = records
        .iter()
        .filter(|r| !verify || r.verify())
        .map(|r| (r.id(), r))
        .filter(|(id, _)| seen.insert(*id))
        .collect();
    let legacy_items: Vec<(RecordId, &Record)> = items
        .iter()
        .copied()
        .filter(|(_, r)| r.is_legacy())
        .collect();
    let legacy = Eval::run_items(cluster, legacy_items.clone(), limit);
    let mut view = legacy.view();
    view.known.extend(items.iter().filter_map(|(_, r)| match r {
        Record::Admission(a) => Some(a.key),
        _ => None,
    }));
    let geneses: Vec<(RecordId, &UserAdmission)> = items
        .iter()
        .filter_map(|(id, r)| match r {
            Record::User(u) if u.is_genesis() => Some((*id, u)),
            _ => None,
        })
        .collect();
    if geneses.is_empty() {
        return Evaluated { view, tier: None };
    }
    // Members for the genesis: without the removals their machine could undo.
    let ranks = legacy.ranks();
    let final_items: Vec<(RecordId, &Record)> = legacy_items
        .iter()
        .copied()
        .filter(|(_, r)| match r {
            Record::Removal(rm) => {
                matches!((ranks.get(&rm.by_key), ranks.get(&rm.key)), (Some(b), Some(k)) if b < k)
            }
            _ => true,
        })
        .collect();
    let cand = Eval::run_items(cluster, final_items, limit);
    let cand_view = cand.view();
    let cand_ranks = cand.ranks();
    let winner = geneses
        .iter()
        .filter_map(|&(id, g)| {
            let m = g.machine.as_ref()?;
            let rank = *cand_ranks.get(&m.key)?;
            (cand_view.members.contains_key(&m.key) && g.clock > rank.0).then_some((
                (rank, g.clock, id),
                id,
                g,
            ))
        })
        .min_by_key(|(order, _, _)| *order);
    let Some((_, gid, genesis)) = winner else {
        return Evaluated { view, tier: None };
    };
    // Users: as members, from the genesis.
    let mut synth: Vec<(RecordId, Record)> = Vec::new();
    let mut real: HashMap<RecordId, &Record> = HashMap::new();
    for &(id, r) in &items {
        let s = match r {
            Record::User(u) if u.is_genesis() && id != gid => continue,
            // Only the genesis admits its own key.
            Record::User(u) if u.by_key == u.key && id != gid => continue,
            Record::User(u) => Record::Admission(Admission {
                node: u.name.clone(),
                key: u.key,
                address: String::new(),
                by: u.by.clone(),
                by_key: u.by_key,
                at: u.at,
                clock: u.clock,
                nonce: String::new(),
                pool: None,
                sig: Signature([0; 64]),
            }),
            Record::UserRemoval(rm) => Record::Removal(Removal {
                node: rm.name.clone(),
                key: rm.key,
                by: rm.by.clone(),
                by_key: rm.by_key,
                at: rm.at,
                clock: rm.clock,
                kept: rm.kept.clone(),
                by_user: false,
                sig: Signature([0; 64]),
            }),
            _ => continue,
        };
        real.insert(id, r);
        synth.push((id, s));
    }
    let users = Eval::run_items(
        &genesis.key,
        synth.iter().map(|(id, r)| (*id, r)).collect(),
        limit,
    );
    let mut tier = MachineTier::default();
    let mut valid: BTreeSet<RecordId> = cand_view.valid.clone();
    valid.insert(gid);
    let mut user_members = BTreeMap::new();
    let mut user_removed = BTreeMap::new();
    for (i, r) in users.recs.iter().enumerate() {
        if users.lab[i] != Label::In {
            continue;
        }
        let id = users.ids[i];
        valid.insert(id);
        match (r, real[&id]) {
            (Record::Admission(a), Record::User(u)) => {
                tier.user_reach.entry(a.key).or_insert(a.clock);
                user_members.entry(a.key).or_insert_with(|| u.clone());
            }
            (Record::Removal(rm), Record::UserRemoval(ur)) => {
                tier.user_kept
                    .entry(rm.key)
                    .or_default()
                    .push(rm.kept.iter().copied().collect());
                user_removed.entry(rm.key).or_insert_with(|| ur.clone());
            }
            _ => {}
        }
    }
    user_members.retain(|k, _| !user_removed.contains_key(k));
    // Machines: what users signed.
    let mut machine_recs: Vec<(u64, RecordId, &Record)> = items
        .iter()
        .filter(|(_, r)| !r.is_legacy() && matches!(r, Record::Admission(_) | Record::Removal(_)))
        .map(|&(id, r)| (r.clock(), id, r))
        .collect();
    machine_recs.sort_by_key(|&(c, id, _)| (c, id));
    let mut removed: BTreeMap<PublicKey, Removal> = BTreeMap::new();
    let mut released: HashMap<PublicKey, u64> = HashMap::new();
    for &(clock, id, r) in &machine_recs {
        if let Record::Removal(rm) = r
            && tier.signed_ok(id, &rm.by_key, clock, false)
        {
            valid.insert(id);
            removed.entry(rm.key).or_insert_with(|| rm.clone());
            released.entry(rm.key).or_insert(clock);
        }
    }
    let mut members: BTreeMap<PublicKey, Admission> = BTreeMap::new();
    let mut by_name: HashMap<&str, Vec<PublicKey>> = HashMap::new();
    let mut taking: HashSet<PublicKey> = HashSet::new();
    let mut release_at: std::collections::BinaryHeap<std::cmp::Reverse<(u64, PublicKey)>> =
        Default::default();
    for &(clock, id, r) in &machine_recs {
        let Record::Admission(a) = r else { continue };
        while let Some(std::cmp::Reverse((at, k))) = release_at.peek().copied()
            && at < clock
        {
            release_at.pop();
            taking.remove(&k);
        }
        if !tier.signed_ok(id, &a.by_key, clock, true) {
            continue;
        }
        let free = |k: &PublicKey| released.get(k).is_some_and(|&c| c < clock);
        if by_name
            .get(a.node.as_str())
            .is_some_and(|ks| ks.iter().any(|k| *k != a.key && !free(k)))
        {
            tier.name_taken.insert(id);
            continue;
        }
        if taking.len() - usize::from(taking.contains(&a.key)) >= limit {
            tier.no_room.insert(id);
            continue;
        }
        valid.insert(id);
        let holders = by_name.entry(a.node.as_str()).or_default();
        if !holders.contains(&a.key) {
            holders.push(a.key);
        }
        if taking.insert(a.key) {
            let at = released.get(&a.key).copied().unwrap_or(u64::MAX);
            release_at.push(std::cmp::Reverse((at, a.key)));
        }
        if !removed.contains_key(&a.key) {
            members.entry(a.key).or_insert_with(|| a.clone());
        }
    }
    view.members = members;
    view.removed = removed;
    view.valid = valid;
    view.users = user_members;
    view.users_removed = user_removed;
    view.genesis = Some(gid);
    Evaluated {
        view,
        tier: Some(tier),
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Label {
    Open,
    In,
    Out,
}

/// Whether an admission's name is free.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum NameState {
    Free,
    Taken,
    Unknown,
}

#[cfg(test)]
thread_local! {
    /// Tests set it to label records with [`Eval::propagate_slow`].
    static SLOW_PROPAGATE: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
    /// Passes [`Eval::propagate`] made over the records.
    static PASSES: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
}

/// The labelling of a record set: which records count (`In`) and which are void (`Out`).
struct Eval<'a> {
    recs: Vec<&'a Record>,
    ids: Vec<RecordId>,
    founding: Option<usize>,
    founder: PublicKey,
    /// Admissions by the key they admit, removals by the key they remove.
    adm_of: HashMap<PublicKey, Vec<usize>>,
    rem_of: HashMap<PublicKey, Vec<usize>>,
    by_name: HashMap<&'a str, Vec<usize>>,
    /// For each record, the removals that would void it (removals of its signer that don't
    /// list it).
    attackers: Vec<Vec<usize>>,
    lab: Vec<Label>,
    /// Room for this many members ([`MAX_MEMBERS`]).
    limit: usize,
}

/// The keys that take room toward [`MAX_MEMBERS`] at a point of the records in order: those with
/// an earlier admission that counts, and no removal that counts with a lower clock than the point.
/// Kept for the labels so far as bounds: `lower` counts keys that surely take room (an admission
/// in, no removal that may count), `upper` those that may (an admission not out, no removal in).
#[derive(Default)]
struct Room {
    adm_in: HashMap<PublicKey, u32>,
    adm_maybe: HashMap<PublicKey, u32>,
    rel_in: HashSet<PublicKey>,
    rel_maybe: HashSet<PublicKey>,
    lower: usize,
    upper: usize,
    /// Removals at `clock`, which release their key only for records with a higher clock.
    pending: Vec<usize>,
    clock: u64,
}

/// The lowest admission clock of each key whose admissions pass `ok`, reaching out from the
/// founder: a key is reached by an admission whose signer is reached with a lower clock.
type Reach = HashMap<PublicKey, (u64, RecordId)>;

impl<'a> Eval<'a> {
    /// Labels the records members signed (v2) among `records`; with `verify`, records whose
    /// signature is bad are left out first.
    fn run(cluster: &PublicKey, records: &'a [Record], verify: bool, limit: usize) -> Eval<'a> {
        let items = records
            .iter()
            .filter(|r| r.is_legacy() && (!verify || r.verify()))
            .map(|r| (r.id(), r))
            .collect();
        Eval::run_items(cluster, items, limit)
    }

    /// Labels `items` (admissions and removals, with their ids), `cluster` as founder: the
    /// records members signed, or users' records shaped as such ([`evaluate`]).
    fn run_items(
        cluster: &PublicKey,
        items: Vec<(RecordId, &'a Record)>,
        limit: usize,
    ) -> Eval<'a> {
        let mut seen = HashSet::new();
        let mut recs: Vec<(u64, RecordId, &Record)> = items
            .into_iter()
            .filter(|(_, r)| matches!(r, Record::Admission(_) | Record::Removal(_)))
            .filter(|(id, _)| seen.insert(*id))
            .map(|(id, r)| (r.clock(), id, r))
            .collect();
        recs.sort_by_key(|r| (r.0, r.1));
        let ids: Vec<RecordId> = recs.iter().map(|r| r.1).collect();
        let recs: Vec<&Record> = recs.into_iter().map(|r| r.2).collect();
        // The first by clock, then id, should the founder have founded again (its records lost,
        // its key not): the pick must not depend on the order records arrived in.
        let founding = recs.iter().position(
            |r| matches!(r, Record::Admission(a) if a.key == *cluster && a.is_founding()),
        );
        let mut adm_of: HashMap<PublicKey, Vec<usize>> = HashMap::new();
        let mut rem_of: HashMap<PublicKey, Vec<usize>> = HashMap::new();
        let mut by_name: HashMap<&str, Vec<usize>> = HashMap::new();
        let mut kept: HashMap<usize, HashSet<RecordId>> = HashMap::new();
        for (i, r) in recs.iter().enumerate() {
            match r {
                Record::Admission(a) => {
                    adm_of.entry(a.key).or_default().push(i);
                    by_name.entry(a.node.as_str()).or_default().push(i);
                }
                Record::Removal(rm) => {
                    rem_of.entry(rm.key).or_default().push(i);
                    kept.insert(i, rm.kept.iter().copied().collect());
                }
                Record::User(_) | Record::UserRemoval(_) => {}
            }
        }
        let attackers = recs
            .iter()
            .enumerate()
            .map(|(i, r)| {
                rem_of
                    .get(&r.signer())
                    .map(|rs| {
                        rs.iter()
                            .copied()
                            .filter(|&j| voids(ids[j], &kept[&j], ids[i]))
                            .collect()
                    })
                    .unwrap_or_default()
            })
            .collect();
        let mut lab = vec![Label::Open; recs.len()];
        // A record set without the founding record has no members at all.
        if founding.is_none() {
            lab.fill(Label::Out);
        }
        let mut e = Eval {
            recs,
            ids,
            founding,
            founder: *cluster,
            adm_of,
            rem_of,
            by_name,
            attackers,
            lab,
            limit,
        };
        e.settle();
        e
    }

    fn is_admission(&self, i: usize) -> bool {
        matches!(self.recs[i], Record::Admission(_))
    }

    /// Each key's first admission that counts, by clock then id: where it ranks when removals
    /// void each other (see [`Trust::ranks`]).
    fn ranks(&self) -> HashMap<PublicKey, (u64, RecordId)> {
        let mut ranks = HashMap::new();
        for (i, r) in self.recs.iter().enumerate() {
            if let Record::Admission(a) = r
                && self.lab[i] == Label::In
            {
                ranks.entry(a.key).or_insert((a.clock, self.ids[i]));
            }
        }
        ranks
    }

    fn reach(&self, lab: &[Label], ok: impl Fn(Label) -> bool) -> Reach {
        let mut reach = self.reach_from(lab, &ok);
        for i in 0..self.recs.len() {
            self.reach_on(i, lab, &ok, &mut reach);
        }
        reach
    }

    /// Where [`Eval::reach`] starts: the founder, if its founding record passes `ok`.
    fn reach_from(&self, lab: &[Label], ok: impl Fn(Label) -> bool) -> Reach {
        let mut reach = Reach::new();
        if let Some(f) = self.founding
            && ok(lab[f])
        {
            reach.insert(self.founder, (self.recs[f].clock(), self.ids[f]));
        }
        reach
    }

    /// [`Eval::reach`]'s step at record `i`. Records are sorted by clock, and an admission must
    /// have a higher clock than the one that reached its signer, so one pass in order is enough.
    fn reach_on(&self, i: usize, lab: &[Label], ok: impl Fn(Label) -> bool, reach: &mut Reach) {
        if Some(i) == self.founding {
            // The founder's own record decides its reach, whatever came before.
            if ok(lab[i]) {
                reach.insert(self.founder, (self.recs[i].clock(), self.ids[i]));
            }
            return;
        }
        let Record::Admission(a) = self.recs[i] else {
            return;
        };
        if !ok(lab[i]) {
            return;
        }
        let Some(&(c, _)) = reach.get(&a.by_key) else {
            return;
        };
        if a.clock > c {
            reach.entry(a.key).or_insert((a.clock, self.ids[i]));
        }
    }

    /// Whether record `i`'s signer is reached (for an admission, with a lower clock).
    fn supported(&self, i: usize, reach: &Reach) -> bool {
        if Some(i) == self.founding {
            return true;
        }
        let r = self.recs[i];
        match reach.get(&r.signer()) {
            None => false,
            Some(&(c, _)) => !self.is_admission(i) || r.clock() > c,
        }
    }

    /// [`Room`] before record `i`: removals with a lower clock than `i`'s release their keys.
    fn room_to(&self, room: &mut Room, i: usize, lab: &[Label]) {
        let clock = self.recs[i].clock();
        if clock == room.clock && i > 0 {
            return;
        }
        for r in std::mem::take(&mut room.pending) {
            let Record::Removal(rm) = self.recs[r] else {
                continue;
            };
            if lab[r] == Label::In
                && room.rel_in.insert(rm.key)
                && room.adm_maybe.get(&rm.key).is_some_and(|&n| n > 0)
            {
                room.upper -= 1;
            }
            if lab[r] != Label::Out
                && room.rel_maybe.insert(rm.key)
                && room.adm_in.get(&rm.key).is_some_and(|&n| n > 0)
            {
                room.lower -= 1;
            }
        }
        room.clock = clock;
    }

    /// [`Room`] after record `i`, labelled.
    fn room_add(&self, room: &mut Room, i: usize, lab: &[Label]) {
        match self.recs[i] {
            Record::Removal(_) => room.pending.push(i),
            Record::User(_) | Record::UserRemoval(_) => {}
            Record::Admission(a) => {
                if lab[i] == Label::In {
                    let n = room.adm_in.entry(a.key).or_default();
                    *n += 1;
                    if *n == 1 && !room.rel_maybe.contains(&a.key) {
                        room.lower += 1;
                    }
                }
                if lab[i] != Label::Out {
                    let n = room.adm_maybe.entry(a.key).or_default();
                    *n += 1;
                    if *n == 1 && !room.rel_in.contains(&a.key) {
                        room.upper += 1;
                    }
                }
            }
        }
    }

    /// Whether there is room for admission `i`'s key, by [`Room`] just before it.
    fn room_state(&self, room: &Room, i: usize) -> NameState {
        let Record::Admission(a) = self.recs[i] else {
            return NameState::Free;
        };
        let lower = room.lower
            - usize::from(
                room.adm_in.get(&a.key).is_some_and(|&n| n > 0) && !room.rel_maybe.contains(&a.key),
            );
        let upper = room.upper
            - usize::from(
                room.adm_maybe.get(&a.key).is_some_and(|&n| n > 0) && !room.rel_in.contains(&a.key),
            );
        if lower >= self.limit {
            NameState::Taken
        } else if upper < self.limit {
            NameState::Free
        } else {
            NameState::Unknown
        }
    }

    /// [`Eval::room_state`] for record `i` by looking at every record before it (tests, and
    /// [`Eval::why_not_member`]).
    fn room_state_at(&self, i: usize, lab: &[Label]) -> NameState {
        let mut room = Room::default();
        for j in 0..i {
            self.room_to(&mut room, j, lab);
            self.room_add(&mut room, j, lab);
        }
        self.room_to(&mut room, i, lab);
        self.room_state(&room, i)
    }

    /// Whether admission `i`'s name is free under `lab`.
    fn name_state(&self, i: usize, lab: &[Label]) -> NameState {
        let Record::Admission(a) = self.recs[i] else {
            return NameState::Free;
        };
        let mut unknown = false;
        for &j in &self.by_name[a.node.as_str()] {
            if j >= i {
                break; // only earlier admissions (by clock, then id) hold a name first
            }
            let Record::Admission(b) = self.recs[j] else {
                continue;
            };
            if b.key == a.key || lab[j] == Label::Out {
                continue;
            }
            // Released if b's key was removed before a (a removal that counts, lower clock).
            let earlier: Vec<usize> = self
                .rem_of
                .get(&b.key)
                .map(|rs| {
                    rs.iter()
                        .copied()
                        .filter(|&r| self.recs[r].clock() < a.clock)
                        .collect()
                })
                .unwrap_or_default();
            if earlier.iter().any(|&r| lab[r] == Label::In) {
                continue;
            }
            if lab[j] == Label::In && earlier.iter().all(|&r| lab[r] == Label::Out) {
                return NameState::Taken;
            }
            unknown = true;
        }
        if unknown {
            NameState::Unknown
        } else {
            NameState::Free
        }
    }

    /// Labels what the labels so far decide, until nothing more is decided. Each pass goes through
    /// the records in clock order and keeps the reach of those before the one it looks at up to
    /// date, so a chain of admissions is decided in one pass, not one pass per link: a store can
    /// hold a chain thousands long (a removed member's), and the view is computed under the
    /// mesh's lock on the store. An admission's signer is reached only by records before it; a
    /// removal's may be reached by any, so for those the reach as the pass began counts too.
    /// Decisions only ever follow from the labels so far, so the result is the same as deciding
    /// one record at a time in any order.
    fn propagate(&self, lab: &mut [Label]) {
        #[cfg(test)]
        if SLOW_PROPAGATE.with(std::cell::Cell::get) {
            return self.propagate_slow(lab);
        }
        let possible_ok = |l: Label| l != Label::Out;
        let definite_ok = |l: Label| l == Label::In;
        loop {
            #[cfg(test)]
            PASSES.with(|p| p.set(p.get() + 1));
            let possible_all = self.reach(lab, possible_ok);
            let definite_all = self.reach(lab, definite_ok);
            let mut possible = self.reach_from(lab, possible_ok);
            let mut definite = self.reach_from(lab, definite_ok);
            let mut room = Room::default();
            let mut changed = false;
            for i in 0..lab.len() {
                self.room_to(&mut room, i, lab);
                if lab[i] == Label::Open {
                    let (can, does) = if self.is_admission(i) {
                        (self.supported(i, &possible), self.supported(i, &definite))
                    } else {
                        (
                            self.supported(i, &possible_all),
                            self.supported(i, &definite) || self.supported(i, &definite_all),
                        )
                    };
                    let attackers = &self.attackers[i];
                    let name = self.name_state(i, lab);
                    let fits = self.room_state(&room, i);
                    if !can
                        || attackers.iter().any(|&r| lab[r] == Label::In)
                        || name == NameState::Taken
                        || fits == NameState::Taken
                    {
                        lab[i] = Label::Out;
                        changed = true;
                    } else if does
                        && attackers.iter().all(|&r| lab[r] == Label::Out)
                        && name == NameState::Free
                        && fits == NameState::Free
                    {
                        lab[i] = Label::In;
                        changed = true;
                    }
                }
                self.reach_on(i, lab, possible_ok, &mut possible);
                self.reach_on(i, lab, definite_ok, &mut definite);
                self.room_add(&mut room, i, lab);
            }
            if !changed {
                return;
            }
        }
    }

    /// [`Eval::propagate`] as it was, one pass per decision along a chain: tests compare the two.
    #[cfg(test)]
    fn propagate_slow(&self, lab: &mut [Label]) {
        loop {
            let possible = self.reach(lab, |l| l != Label::Out);
            let definite = self.reach(lab, |l| l == Label::In);
            let mut changed = false;
            for i in 0..lab.len() {
                if lab[i] != Label::Open {
                    continue;
                }
                let attackers = &self.attackers[i];
                let name = self.name_state(i, lab);
                let fits = self.room_state_at(i, lab);
                if !self.supported(i, &possible)
                    || attackers.iter().any(|&r| lab[r] == Label::In)
                    || name == NameState::Taken
                    || fits == NameState::Taken
                {
                    lab[i] = Label::Out;
                    changed = true;
                } else if self.supported(i, &definite)
                    && attackers.iter().all(|&r| lab[r] == Label::Out)
                    && name == NameState::Free
                    && fits == NameState::Free
                {
                    lab[i] = Label::In;
                    changed = true;
                }
            }
            if !changed {
                return;
            }
        }
    }

    /// Whether every record labelled `In` still meets the conditions to count.
    fn consistent(&self, lab: &[Label]) -> bool {
        let definite = self.reach(lab, |l| l == Label::In);
        (0..lab.len()).filter(|&i| lab[i] == Label::In).all(|i| {
            self.supported(i, &definite)
                && !self.attackers[i].iter().any(|&r| lab[r] == Label::In)
                && self.name_state_strict(i, lab)
        }) && self.room_strict(lab)
    }

    /// For [`Eval::consistent`]: every admission that counts had room, counting only what counts.
    fn room_strict(&self, lab: &[Label]) -> bool {
        // Open as out: only what counts takes room, and only what counts releases it.
        let decided: Vec<Label> = lab
            .iter()
            .map(|&l| if l == Label::Open { Label::Out } else { l })
            .collect();
        let mut room = Room::default();
        for i in 0..decided.len() {
            self.room_to(&mut room, i, &decided);
            if decided[i] == Label::In && self.room_state(&room, i) != NameState::Free {
                return false;
            }
            self.room_add(&mut room, i, &decided);
        }
        true
    }

    /// For [`Eval::consistent`]: no earlier admission that counts holds the name unreleased.
    fn name_state_strict(&self, i: usize, lab: &[Label]) -> bool {
        let Record::Admission(a) = self.recs[i] else {
            return true;
        };
        self.by_name[a.node.as_str()]
            .iter()
            .take_while(|&&j| j < i)
            .all(|&j| match self.recs[j] {
                Record::Admission(b) if b.key != a.key && lab[j] == Label::In => {
                    self.rem_of.get(&b.key).is_some_and(|rs| {
                        rs.iter()
                            .any(|&r| lab[r] == Label::In && self.recs[r].clock() < a.clock)
                    })
                }
                _ => true,
            })
    }

    /// Labels every record: what follows from the records, then cycles of removals by rank.
    fn settle(&mut self) {
        let mut lab = std::mem::take(&mut self.lab);
        self.propagate(&mut lab);
        loop {
            let open: Vec<usize> = (0..lab.len()).filter(|&i| lab[i] == Label::Open).collect();
            if open.is_empty() {
                break;
            }
            // The removal signed by the member admitted earliest goes first (the contested-removal
            // rule, with [`voids`]); an admission only if no removal is open.
            let possible = self.reach(&lab, |l| l != Label::Out);
            let rank = |i: usize| {
                let r = self.recs[i];
                let signer = possible
                    .get(&r.signer())
                    .copied()
                    .unwrap_or((u64::MAX, RecordId([0xff; 32])));
                (self.is_admission(i), signer, r.clock(), self.ids[i])
            };
            let pick = open.into_iter().min_by_key(|&i| rank(i)).expect("open");
            let mut trial = lab.clone();
            trial[pick] = Label::In;
            // The removals in conflict with it lose.
            for &r in &self.attackers[pick] {
                if trial[r] == Label::Open {
                    trial[r] = Label::Out;
                }
            }
            self.propagate(&mut trial);
            if self.consistent(&trial) {
                lab = trial;
            } else {
                lab[pick] = Label::Out;
                self.propagate(&mut lab);
            }
        }
        self.lab = lab;
    }

    fn view(&self) -> View {
        let mut view = View {
            known: self.adm_of.keys().copied().collect(),
            ..View::default()
        };
        for (i, r) in self.recs.iter().enumerate() {
            if self.lab[i] != Label::In {
                continue;
            }
            view.valid.insert(self.ids[i]);
            if let Record::Removal(rm) = r {
                view.removed.entry(rm.key).or_insert_with(|| rm.clone());
            }
        }
        for (i, r) in self.recs.iter().enumerate() {
            if let Record::Admission(a) = r
                && self.lab[i] == Label::In
                && !view.removed.contains_key(&a.key)
            {
                view.members.entry(a.key).or_insert_with(|| a.clone());
            }
        }
        view
    }

    /// Why `key` is not a member, in plain words.
    fn why_not_member(&self, key: &PublicKey) -> String {
        if let Some(&r) = self
            .rem_of
            .get(key)
            .and_then(|rs| rs.iter().find(|&&r| self.lab[r] == Label::In))
            && let Record::Removal(rm) = self.recs[r]
        {
            return format!("it was removed by {}", rm.by);
        }
        let Some(adms) = self.adm_of.get(key) else {
            return "no admission of it is known".into();
        };
        let definite = self.reach(&self.lab, |l| l == Label::In);
        for &i in adms {
            let Record::Admission(a) = self.recs[i] else {
                continue;
            };
            if self.attackers[i].iter().any(|&r| self.lab[r] == Label::In) {
                return format!(
                    "{} admitted it, and was removed by a member that had not heard of the admission",
                    a.by
                );
            }
            if self.name_state(i, &self.lab) == NameState::Taken {
                return format!(
                    "another machine was admitted under the name {} first",
                    a.node
                );
            }
            if self.room_state_at(i, &self.lab) == NameState::Taken {
                return format!(
                    "the cluster had {} members, the most it can have, when {} admitted it",
                    self.limit, a.by
                );
            }
            if !self.supported(i, &definite) {
                return format!("{}, which admitted it, is no longer in the cluster", a.by);
            }
        }
        "its admission no longer counts".into()
    }
}

/// A pairing in progress on the new machine: `pair` made a request with this nonce.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Pending {
    pub nonce: String,
    pub at: Millis,
}

/// What `keys/members.json` holds.
#[derive(Clone, Debug, Serialize, Deserialize)]
struct StateFile {
    cluster: PublicKey,
    records: Vec<Record>,
    /// Latest known address of each member (from hellos), when it differs from its admission.
    #[serde(default)]
    addresses: BTreeMap<PublicKey, String>,
    #[serde(default)]
    pending: Option<Pending>,
}

/// The trust records, the view they imply, and the address book, persisted under `keys/`. It
/// holds only records whose signature is good.
#[derive(Clone, Debug)]
pub struct Trust {
    path: Option<PathBuf>,
    pub cluster: PublicKey,
    records: Vec<Record>,
    pub addresses: BTreeMap<PublicKey, String>,
    pub pending: Option<Pending>,
    pub view: View,
    /// Of the records, kept up to date by `recompute`.
    digest: String,
    /// Batches of records turned away because all of them are void in the view with them (by
    /// [`batch_key`] of what [`Trust::screen`] let through), so a member sending the same records
    /// again isn't checked again; forgotten whenever the records change. Whole batches, not
    /// records: a record void with the records held may not be void beside others not held yet
    /// (one that voids the removal voiding it).
    refused: HashSet<[u8; 32]>,
    /// This machine's key (None for a read-only store): the records it signed and those admitting
    /// it are kept even when they are void, as what voids them may turn out void itself, and
    /// nobody else need hold them.
    me: Option<PublicKey>,
    /// Room for this many members: [`MAX_MEMBERS`] (tests use fewer).
    limit: usize,
}

/// What a merge changed, for events.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Changes {
    pub joined: Vec<Admission>,
    pub left: Vec<Admission>,
    /// The records changed (the caller saves them).
    pub stored: bool,
}

impl Changes {
    pub fn is_empty(&self) -> bool {
        self.joined.is_empty() && self.left.is_empty()
    }
}

impl Trust {
    /// Loads the records from `dir`. Without any, this machine founds a cluster of its own: a
    /// self-signed admission, saved.
    pub fn load_or_found(
        dir: &Path,
        id: &Identity,
        name: &str,
        address: &str,
    ) -> Result<Trust, MeshError> {
        let path = dir.join(STATE_FILE);
        match std::fs::read(&path) {
            Ok(bytes) => {
                let f: StateFile = serde_json::from_slice(&bytes)
                    .map_err(|e| MeshError::Identity(format!("{}: {e}", path.display())))?;
                let mut t = Trust {
                    path: Some(path),
                    cluster: f.cluster,
                    records: verified(f.records),
                    addresses: f.addresses,
                    pending: f.pending,
                    view: View::default(),
                    digest: String::new(),
                    refused: HashSet::new(),
                    me: Some(id.public()),
                    limit: MAX_MEMBERS,
                };
                t.recompute();
                // Void records taken before they turned void go now. A save that fails (a full
                // disk) leaves them on disk until the next one.
                if !t.prune(None).is_empty() {
                    let _ = t.save();
                }
                Ok(t)
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                let mut t = Trust::found(id, name, address);
                t.path = Some(path);
                t.save()?;
                Ok(t)
            }
            Err(e) => Err(MeshError::Identity(format!("{}: {e}", path.display()))),
        }
    }

    /// Reads the records without creating or ever writing them (for short-lived clients).
    pub fn load_read_only(dir: &Path) -> Result<Trust, MeshError> {
        let path = dir.join(STATE_FILE);
        let bytes = std::fs::read(&path).map_err(|e| {
            MeshError::Identity(format!(
                "{}: {e} (has the daemon run here?)",
                path.display()
            ))
        })?;
        let f: StateFile = serde_json::from_slice(&bytes)
            .map_err(|e| MeshError::Identity(format!("{}: {e}", path.display())))?;
        let mut t = Trust {
            path: None,
            cluster: f.cluster,
            records: verified(f.records),
            addresses: f.addresses,
            pending: None,
            view: View::default(),
            digest: String::new(),
            refused: HashSet::new(),
            me: None,
            limit: MAX_MEMBERS,
        };
        t.recompute();
        Ok(t)
    }

    /// A new cluster with this machine as its only member.
    pub fn found(id: &Identity, name: &str, address: &str) -> Trust {
        let founding = Admission::sign(id, name, name, id.public(), address, "", 0);
        let mut t = Trust {
            path: None,
            cluster: id.public(),
            records: vec![Record::Admission(founding)],
            addresses: BTreeMap::new(),
            pending: None,
            view: View::default(),
            digest: String::new(),
            refused: HashSet::new(),
            me: Some(id.public()),
            limit: MAX_MEMBERS,
        };
        t.recompute();
        t
    }

    /// Starts over as a cluster of one (after this machine's key was removed and replaced).
    pub fn refound(&mut self, id: &Identity, name: &str, address: &str) -> Result<(), MeshError> {
        let path = self.path.take();
        *self = Trust::found(id, name, address);
        self.path = path;
        self.save()
    }

    pub fn save(&self) -> Result<(), MeshError> {
        let Some(path) = &self.path else {
            return Ok(());
        };
        let f = StateFile {
            cluster: self.cluster,
            records: self.records.clone(),
            addresses: self.addresses.clone(),
            pending: self.pending.clone(),
        };
        let bytes = serde_json::to_vec_pretty(&f).expect("serializable");
        let dir = path.parent().expect("state file has a directory");
        let name = path.file_name().expect("file name").to_string_lossy();
        write_private(dir, &name, &bytes)
    }

    pub fn records(&self) -> &[Record] {
        &self.records
    }

    /// The records that count: what a machine refused as no member needs to see where it stands.
    /// Without the records held that count nowhere.
    pub fn counting(&self) -> Vec<Record> {
        self.records
            .iter()
            .filter(|r| self.view.valid.contains(&r.id()))
            .cloned()
            .collect()
    }

    /// Of the records that count, those that make the members members: each member's admission
    /// that counts, the admissions back from its signer to the founder's, and for a removed key
    /// among those signers its removal that counts, with the admissions back from that
    /// removal's signer, and so on. They imply the same members as all the records held, and are
    /// few: what a pairing reply carries (removed machines' records count for good, and the
    /// reply is pasted). The machine joining takes the rest when it first compares records with
    /// a member.
    ///
    /// In a cluster with users: the genesis and the admissions back from its co-signer to the
    /// founder's (as above), every user's record that counts, and each member's admission.
    pub fn members_proof(&self) -> Vec<Record> {
        let mut want: HashSet<RecordId>;
        match self.genesis() {
            None => {
                let mut keys: Vec<PublicKey> = self.view.members.keys().copied().collect();
                keys.push(self.cluster);
                want = self.legacy_proof(keys);
            }
            Some(g) => {
                let cosigner = g.machine.as_ref().map(|m| m.key).unwrap_or(g.key);
                want = self.legacy_proof(vec![cosigner, self.cluster]);
                want.extend(self.records.iter().filter_map(|r| match r {
                    Record::User(_) | Record::UserRemoval(_) => {
                        Some(r.id()).filter(|id| self.view.valid.contains(id))
                    }
                    _ => None,
                }));
                want.extend(self.view.members.values().map(Admission::id));
            }
        }
        self.records
            .iter()
            .filter(|r| {
                let id = r.id();
                want.contains(&id)
                    || matches!(r, Record::Admission(a) if a.key == self.cluster && a.is_founding()
                        && self.view.valid.contains(&id))
            })
            .cloned()
            .collect()
    }

    /// The ids of the members' records (v2) that make `keys` members, as
    /// [`Trust::members_proof`] says.
    fn legacy_proof(&self, mut keys: Vec<PublicKey>) -> HashSet<RecordId> {
        // Each key's first admission and removal that count, by clock then id: the ones the
        // view names, and the first admission is the one its chain of signers starts from.
        let mut adm: HashMap<PublicKey, (u64, RecordId, &Admission)> = HashMap::new();
        let mut rem: HashMap<PublicKey, (u64, RecordId, &Removal)> = HashMap::new();
        for r in self.records.iter().filter(|r| r.is_legacy()) {
            let id = r.id();
            if !self.view.valid.contains(&id) {
                continue;
            }
            match r {
                Record::Admission(a) => {
                    let e = adm.entry(a.key).or_insert((a.clock, id, a));
                    if (a.clock, id) < (e.0, e.1) {
                        *e = (a.clock, id, a);
                    }
                }
                Record::Removal(rm) => {
                    let e = rem.entry(rm.key).or_insert((rm.clock, id, rm));
                    if (rm.clock, id) < (e.0, e.1) {
                        *e = (rm.clock, id, rm);
                    }
                }
                Record::User(_) | Record::UserRemoval(_) => {}
            }
        }
        let mut want: HashSet<RecordId> = HashSet::new();
        let mut seen: HashSet<PublicKey> = HashSet::new();
        while let Some(k) = keys.pop() {
            if !seen.insert(k) {
                continue;
            }
            if let Some(&(_, id, a)) = adm.get(&k) {
                want.insert(id);
                keys.push(a.by_key);
            }
            if let Some(&(_, id, rm)) = rem.get(&k) {
                want.insert(id);
                keys.push(rm.by_key);
            }
        }
        want
    }

    /// The first user's admission that counts, if users exist.
    pub fn genesis(&self) -> Option<&UserAdmission> {
        let g = self.view.genesis?;
        self.records.iter().find_map(|r| match r {
            Record::User(u) if u.is_genesis() && u.id() == g => Some(u),
            _ => None,
        })
    }

    /// Whether users sign what counts here (a first user's admission counts): from then on only
    /// what users sign admits or removes machines.
    pub fn users_exist(&self) -> bool {
        self.view.users_exist()
    }

    /// Whether `key` is a user's key that counts: admitted by a user (or the first user) and not
    /// removed.
    pub fn recognized_user(&self, key: &PublicKey) -> bool {
        self.view.users.contains_key(key)
    }

    /// The pool of member `key`, from the admission that made it a member ([`DEFAULT_POOL`] for
    /// one a member signed); None for a key that is no member.
    pub fn pool_of(&self, key: &PublicKey) -> Option<&str> {
        self.view.members.get(key).map(Admission::pool_name)
    }

    fn recompute(&mut self) {
        self.view = View::of_verified(&self.cluster, &self.records, self.limit);
        self.digest = digest_of(&self.cluster, &self.records);
    }

    /// Drops the void records ([`void_records`]) among `only` (all of them if None), but this
    /// machine's own and those admitting it (should one count again, its signer holds it), as
    /// far as the view without them stays the same: which records count in a cycle of removals
    /// can depend on records that end up counting nowhere, so a store drops none that would
    /// change its view. When dropping them all would, it drops each half that doesn't, and so on
    /// down, so one such record doesn't keep the others (a removed machine's junk sent beside
    /// one). A merge drops only records arriving: a held record that turned void stays until
    /// the store is next loaded, as a void record can count again (the removal voiding it voided
    /// in turn), and a member that dropped it would get it back only from a member that kept it;
    /// stores that kept dropping held records could go on taking and dropping them, exchange
    /// after exchange, and never agree. Records that count nowhere but aren't void always stay:
    /// beside another member's records they may count. Returns their ids.
    fn prune(&mut self, only: Option<&HashSet<RecordId>>) -> Vec<RecordId> {
        let me = self.me;
        let mine = |r: &Record| {
            me.is_some_and(|me| {
                r.signer() == me || matches!(r, Record::Admission(a) if a.key == me)
            })
        };
        let mut void: Vec<RecordId> = void_records(&self.cluster, &self.records, &self.view, mine)
            .into_iter()
            .filter(|id| only.is_none_or(|only| only.contains(id)))
            .collect();
        void.sort();
        let mut dropped = Vec::new();
        let mut groups = vec![void];
        while let Some(group) = groups.pop() {
            if group.is_empty() {
                continue;
            }
            let drop: HashSet<RecordId> = group.iter().copied().collect();
            let rest: Vec<Record> = self
                .records
                .iter()
                .filter(|r| !drop.contains(&r.id()))
                .cloned()
                .collect();
            let view = View::of_verified(&self.cluster, &rest, self.limit);
            if view.standing() == self.view.standing() {
                self.records = rest;
                self.view = view;
                dropped.extend(group);
            } else if group.len() > 1 {
                let (a, b) = group.split_at(group.len() / 2);
                groups.push(b.to_vec());
                groups.push(a.to_vec());
            }
        }
        if !dropped.is_empty() {
            self.digest = digest_of(&self.cluster, &self.records);
        }
        dropped
    }

    /// The logical time for a new record: one more than the highest clock among the records
    /// that count (a void record can't push it up).
    pub fn next_clock(&self) -> u64 {
        let latest = self
            .records
            .iter()
            .filter(|r| self.view.valid.contains(&r.id()))
            .map(Record::clock)
            .max()
            .unwrap_or(0);
        latest.saturating_add(1)
    }

    /// The ids of the records signed by `key` that count, and of those admitting this machine:
    /// what a removal of `key` keeps. Not the others held: a key `key` admitted with a record
    /// that counts nowhere (past the limit, under a name taken) could then go on signing
    /// records every member keeps, with `key` removed and itself no member to remove.
    pub fn signed_by(&self, key: &PublicKey) -> Vec<RecordId> {
        let mut ids: Vec<RecordId> = self
            .records
            .iter()
            .filter(|r| r.signer() == *key)
            .filter(|r| {
                self.view.valid.contains(&r.id())
                    || matches!(r, Record::Admission(a) if Some(a.key) == self.me)
            })
            .map(Record::id)
            .collect();
        ids.sort();
        ids.dedup();
        ids
    }

    /// Where each key ranks when removals void each other (lower first): its earliest admission
    /// a member signed (v2) that counts, by clock then id, the founder's founding first. Keys no
    /// such admission that counts admits are missing.
    pub fn ranks(&self) -> HashMap<PublicKey, (u64, RecordId)> {
        let mut ranks: HashMap<PublicKey, (u64, RecordId)> = HashMap::new();
        for r in &self.records {
            if let Record::Admission(a) = r
                && !a.by_user()
            {
                let id = a.id();
                if self.view.valid.contains(&id) {
                    let rank = ranks.entry(a.key).or_insert((a.clock, id));
                    *rank = (*rank).min((a.clock, id));
                }
            }
        }
        ranks
    }

    /// Whether a removal of `key` that counts is final: signed by a user, or by a member ranked
    /// before `key`, which `key` can't undo by removing it back.
    pub fn removed_for_good(&self, key: &PublicKey) -> bool {
        if self.view.removed.get(key).is_some_and(|r| r.by_user) {
            return true;
        }
        let ranks = self.ranks();
        let Some(rank) = ranks.get(key) else {
            return false;
        };
        self.records.iter().any(|r| match r {
            Record::Removal(rm) if !rm.by_user => {
                rm.key == *key
                    && self.view.valid.contains(&rm.id())
                    && ranks.get(&rm.by_key).is_some_and(|by| by < rank)
            }
            _ => false,
        })
    }

    /// Why `key` is not a member, in plain words.
    pub fn why_not_member(&self, key: &PublicKey) -> String {
        if !self.users_exist() {
            return Eval::run(&self.cluster, &self.records, false, self.limit).why_not_member(key);
        }
        if let Some(rm) = self.view.removed.get(key) {
            return format!("it was removed by {}", rm.by);
        }
        let Some(tier) = evaluate(&self.cluster, &self.records, false, self.limit).tier else {
            return "its admission no longer counts".into();
        };
        let mut adms = self
            .records
            .iter()
            .filter_map(|r| match r {
                Record::Admission(a) if a.key == *key && a.by_user() => Some(a),
                _ => None,
            })
            .peekable();
        if adms.peek().is_none() {
            return "the cluster has users, and no user admitted it".into();
        }
        for a in adms {
            let id = a.id();
            if tier.name_taken.contains(&id) {
                return format!(
                    "another machine was admitted under the name {} first",
                    a.node
                );
            }
            if tier.no_room.contains(&id) {
                return format!(
                    "the cluster had {} members, the most it can have, when {} admitted it",
                    self.limit, a.by
                );
            }
            if !tier.user_reach.contains_key(&a.by_key) {
                return format!("{}, the user that admitted it, is no user here", a.by);
            }
            if !tier.signed_ok(id, &a.by_key, a.clock, true) {
                return format!(
                    "{} admitted it, and was removed by a user that had not seen the admission",
                    a.by
                );
            }
        }
        "its admission no longer counts".into()
    }

    /// Room for this many members.
    pub fn max_members(&self) -> usize {
        self.limit
    }

    /// A digest of the record set: members compare it to see whether they must exchange records.
    pub fn digest(&self) -> String {
        self.digest.clone()
    }

    /// Adds the records not known yet that aren't void in the view of those held and those
    /// arriving, and recomputes the view. Turned away: records with a bad signature, records
    /// signed by a key no record admits, records a removal that counts voids (its signer's,
    /// which doesn't keep them), and the records of keys only such records admit; held records
    /// that turn void stay (see [`Trust::merge_checked`]). The caller saves when
    /// [`Changes::stored`] is set. [`Trust::screen`], [`Checked::new`] and
    /// [`Trust::merge_checked`] in turn, for a caller that holds this store behind a lock:
    /// checking signatures is the slow part.
    pub fn merge(&mut self, incoming: Vec<Record>) -> Changes {
        let screened = self.screen(with_ids(incoming));
        self.merge_checked(Checked::new(screened))
    }

    /// Of `incoming` (records with their ids, from [`with_ids`]), those [`Trust::merge`] may
    /// take, before their signatures are checked: not held, and signed by a key a held record
    /// admits or one admitted so among them; none if those are a batch turned away before. Cheap
    /// (no signature is checked), so a member sending records in bulk, junk included, holds this
    /// store only for lookups.
    pub fn screen(&self, incoming: Vec<(RecordId, Record)>) -> Vec<(RecordId, Record)> {
        let known: HashSet<RecordId> = self.records.iter().map(Record::id).collect();
        let mut admitted = self.admitted();
        // Keys admitted among the records arriving, by keys admitted already, transitively.
        let mut admits: HashMap<(Tier, PublicKey), Vec<(Tier, PublicKey)>> = HashMap::new();
        for (_, r) in &incoming {
            if let Some(a) = r.admits() {
                admits.entry(r.needs()).or_default().push(a);
            }
        }
        let mut reached: Vec<(Tier, PublicKey)> = admitted.iter().copied().collect();
        while let Some(k) = reached.pop() {
            for key in admits.remove(&k).unwrap_or_default() {
                if admitted.insert(key) {
                    reached.push(key);
                }
            }
        }
        let fresh: Vec<(RecordId, Record)> = incoming
            .into_iter()
            .filter(|(id, r)| !known.contains(id) && admitted.contains(&r.needs()))
            .collect();
        if !fresh.is_empty() && self.refused.contains(&batch_key(&fresh)) {
            return Vec::new();
        }
        fresh
    }

    /// [`Trust::merge`] of records [`Trust::screen`] kept and [`Checked::new`] found well signed.
    /// The store may have changed since they were screened; that is looked at again. The view of
    /// every record held and arriving is computed; then the records arriving that are void in it
    /// are dropped as [`Trust::prune`] says (held records stay, even those a removal arriving
    /// voids). A batch none of which is taken is remembered as turned away until the records
    /// change.
    pub fn merge_checked(&mut self, checked: Checked) -> Changes {
        let mut known: HashSet<RecordId> = self.records.iter().map(Record::id).collect();
        let mut admitted = self.admitted();
        let batch = checked.batch;
        let pending: Vec<(RecordId, Record)> = checked
            .records
            .into_iter()
            .filter(|(id, _)| !known.contains(id))
            .collect();
        // A batch may carry a chain (a admits b, b admits c): a record whose signer isn't
        // admitted yet waits until a record taken admits it. In the order they came, those
        // admitted at once first, then each signer's as its admission is taken.
        let mut waiting: HashMap<(Tier, PublicKey), Vec<usize>> = HashMap::new();
        let mut ready: VecDeque<usize> = VecDeque::new();
        for (i, (_, r)) in pending.iter().enumerate() {
            if admitted.contains(&r.needs()) {
                ready.push_back(i);
            } else {
                waiting.entry(r.needs()).or_default().push(i);
            }
        }
        let mut added = Vec::new();
        while let Some(i) = ready.pop_front() {
            let (id, r) = &pending[i];
            if !known.insert(*id) {
                continue;
            }
            if let Some(a) = r.admits()
                && admitted.insert(a)
            {
                ready.extend(waiting.remove(&a).unwrap_or_default());
            }
            self.records.push(r.clone());
            added.push(*id);
        }
        if added.is_empty() {
            return Changes::default();
        }
        let before = self.view.members.clone();
        self.recompute();
        let taken: HashSet<RecordId> = added.iter().copied().collect();
        let dropped = self.prune(Some(&taken));
        let arrived: HashMap<RecordId, &Record> = pending
            .iter()
            .filter(|(id, _)| taken.contains(id))
            .map(|(id, r)| (*id, r))
            .collect();
        if dropped.len() == added.len() && dropped.iter().all(|id| arrived.contains_key(id)) {
            // Nothing changed: every record arriving is void.
            self.refused.insert(batch);
            return Changes::default();
        }
        // The records changed: what was turned away before is looked at again.
        self.refused.clear();
        if !dropped.is_empty() && dropped.iter().all(|id| arrived.contains_key(id)) {
            // The records that arrived and are void, sent again on their own, would be dropped
            // again: the view with them is the one just computed.
            let again: Vec<(RecordId, Record)> = dropped
                .iter()
                .map(|id| (*id, arrived[id].clone()))
                .collect();
            self.refused.insert(batch_key(&again));
        }
        Changes {
            stored: true,
            ..diff(&before, &self.view.members)
        }
    }

    /// The keys a held record admits as signers ([`Record::admits`]), and the founder's.
    fn admitted(&self) -> HashSet<(Tier, PublicKey)> {
        let mut admitted: HashSet<(Tier, PublicKey)> =
            self.records.iter().filter_map(Record::admits).collect();
        admitted.insert((Tier::Machine, self.cluster));
        admitted
    }

    /// The address to reach member `key` on: the latest it gave, or the one it joined with.
    pub fn address_of(&self, key: &PublicKey) -> Option<String> {
        self.addresses.get(key).cloned().or_else(|| {
            self.view
                .members
                .get(key)
                .or_else(|| {
                    self.records.iter().find_map(|r| match r {
                        Record::Admission(a) if a.key == *key => Some(a),
                        _ => None,
                    })
                })
                .map(|a| a.address.clone())
        })
    }

    /// Records the address a member gave in its hello. Returns whether it changed; the caller
    /// saves.
    pub fn set_address(&mut self, key: PublicKey, address: &str) -> bool {
        if self.address_of(&key).as_deref() == Some(address) {
            return false;
        }
        self.addresses.insert(key, address.to_string());
        true
    }

    /// Replaces everything with what a pairing reply carries (this machine joins that cluster).
    /// The caller saves.
    pub fn adopt(&mut self, reply: PairReply) -> Changes {
        let before = self.view.members.clone();
        self.cluster = reply.cluster;
        self.records = Vec::new();
        self.refused.clear();
        self.view = View::default();
        self.merge(reply.records);
        self.addresses = reply.addresses.into_iter().collect();
        self.pending = None;
        self.recompute();
        diff(&before, &self.view.members)
    }
}

/// Of `records`, whose view is `view`, those that are void while it stands: signed by a key
/// that a removal that counts removed without keeping them, and those signed by a key no other
/// record left admits (what a removed machine signed after its removal, and what the keys it
/// admitted then signed). They count nowhere. Records `spare` picks are not void, nor are what
/// they admit. Machines and users are apart: a member's removal (v2) voids what that machine
/// signed, a user's removal what that user signed, and a record's signer must be admitted as
/// what signs it ([`Record::needs`]). A user's removal of a machine voids nothing: machines sign
/// nothing that counts once there are users.
fn void_records(
    cluster: &PublicKey,
    records: &[Record],
    view: &View,
    spare: impl Fn(&Record) -> bool,
) -> HashSet<RecordId> {
    let ids: Vec<RecordId> = records.iter().map(Record::id).collect();
    // The removals that count, by the key they remove: the id, and what it keeps.
    type Kept = Vec<(RecordId, HashSet<RecordId>)>;
    let mut kept: HashMap<(Tier, PublicKey), Kept> = HashMap::new();
    for (r, id) in records.iter().zip(&ids) {
        let removes = match r {
            Record::Removal(rm) if !rm.by_user => Some(((Tier::Machine, rm.key), &rm.kept)),
            Record::UserRemoval(rm) => Some(((Tier::User, rm.key), &rm.kept)),
            _ => None,
        };
        if let Some((key, list)) = removes
            && view.valid.contains(id)
        {
            kept.entry(key)
                .or_default()
                .push((*id, list.iter().copied().collect()));
        }
    }
    let mut admissions: HashMap<(Tier, PublicKey), usize> = HashMap::new();
    let mut by_signer: HashMap<(Tier, PublicKey), Vec<usize>> = HashMap::new();
    for (i, r) in records.iter().enumerate() {
        if let Some(a) = r.admits() {
            *admissions.entry(a).or_default() += 1;
        }
        by_signer.entry(r.needs()).or_default().push(i);
    }
    let founder = (Tier::Machine, *cluster);
    let mut void = vec![false; records.len()];
    let mut queue: Vec<usize> = Vec::new();
    let kill = |i: usize, void: &mut Vec<bool>, queue: &mut Vec<usize>| {
        if !void[i] && !spare(&records[i]) {
            void[i] = true;
            queue.push(i);
        }
    };
    for (i, r) in records.iter().enumerate() {
        let signer = r.needs();
        let voided = kept
            .get(&signer)
            .is_some_and(|ks| ks.iter().any(|(rm, k)| voids(*rm, k, ids[i])));
        let unadmitted = signer != founder && !admissions.contains_key(&signer);
        if voided || unadmitted {
            kill(i, &mut void, &mut queue);
        }
    }
    while let Some(i) = queue.pop() {
        let Some(a) = records[i].admits() else {
            continue;
        };
        let left = admissions.get_mut(&a).expect("counted");
        *left -= 1;
        if *left == 0 && a != founder {
            for &j in by_signer.get(&a).map_or(&[][..], Vec::as_slice) {
                kill(j, &mut void, &mut queue);
            }
        }
    }
    (0..records.len())
        .filter(|&i| void[i])
        .map(|i| ids[i])
        .collect()
}

/// Whether a removal (id `removal`, keeping `kept`) voids record `record`, signed by the key it
/// removes: unless it keeps it, or is it. The contested-removal rule lives here and in the rank
/// [`Eval::settle`] picks by: removals that void each other (crossing removals, longer cycles)
/// leave cycles that rank settles (the one signed by the member admitted earliest counts).
fn voids(removal: RecordId, kept: &HashSet<RecordId>, record: RecordId) -> bool {
    removal != record && !kept.contains(&record)
}

/// The digest of a set of records in `cluster` ([`Trust::digest`] of a [`Trust`] holding them).
pub fn digest_of(cluster: &PublicKey, records: &[Record]) -> String {
    let mut ids: Vec<RecordId> = records.iter().map(Record::id).collect();
    ids.sort();
    let mut ctx = ring::digest::Context::new(&ring::digest::SHA256);
    ctx.update(cluster.0.as_slice());
    for id in ids {
        ctx.update(&id.0);
    }
    B64.encode(ctx.finish().as_ref())
}

/// The records whose signature is good: a [`Trust`] holds only those, so it doesn't check them
/// again each time it computes its view.
fn verified(records: Vec<Record>) -> Vec<Record> {
    records.into_iter().filter(Record::verify).collect()
}

/// Each record with its id, for [`Trust::screen`]. An id takes a hash of the record, so a caller
/// holding the store behind a lock makes them before it takes the lock.
pub fn with_ids(records: Vec<Record>) -> Vec<(RecordId, Record)> {
    records.into_iter().map(|r| (r.id(), r)).collect()
}

/// Records [`Trust::screen`] kept whose signatures are good: what [`Trust::merge_checked`] takes.
/// Checking a signature is the slow part of a merge (tens of microseconds each), so a caller
/// holding the store behind a lock checks them without it.
pub struct Checked {
    records: Vec<(RecordId, Record)>,
    /// The [`batch_key`] of what was screened, signatures good or bad.
    batch: [u8; 32],
}

impl Checked {
    pub fn new(screened: Vec<(RecordId, Record)>) -> Checked {
        Checked {
            batch: batch_key(&screened),
            records: screened.into_iter().filter(|(_, r)| r.verify()).collect(),
        }
    }
}

/// A key for a batch of records as a whole: over each record's id and signature (a copy of a
/// record with a bad signature has the record's id, so a batch turned away for holding one
/// doesn't stand for the batch holding the record itself).
fn batch_key(records: &[(RecordId, Record)]) -> [u8; 32] {
    let mut parts: Vec<(&RecordId, &Signature)> =
        records.iter().map(|(id, r)| (id, r.sig())).collect();
    parts.sort();
    parts.dedup();
    let mut ctx = ring::digest::Context::new(&ring::digest::SHA256);
    for (id, sig) in parts {
        ctx.update(&id.0);
        ctx.update(&sig.0);
    }
    let mut out = [0u8; 32];
    out.copy_from_slice(ctx.finish().as_ref());
    out
}

fn diff(
    before: &BTreeMap<PublicKey, Admission>,
    after: &BTreeMap<PublicKey, Admission>,
) -> Changes {
    Changes {
        stored: false,
        joined: after
            .iter()
            .filter(|(k, _)| !before.contains_key(k))
            .map(|(_, a)| a.clone())
            .collect(),
        left: before
            .iter()
            .filter(|(k, _)| !after.contains_key(k))
            .map(|(_, a)| a.clone())
            .collect(),
    }
}

/// What `potluck pair` prints on the new machine.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PairRequest {
    pub v: u32,
    pub name: NodeName,
    pub key: PublicKey,
    pub address: String,
    pub version: String,
    pub nonce: String,
}

/// What `potluck admit` prints on the member: the signed admission among the records that make
/// the members members ([`Trust::members_proof`]), and the members' latest addresses.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PairReply {
    pub v: u32,
    pub cluster: PublicKey,
    /// The new machine's key and the nonce of its request.
    pub key: PublicKey,
    pub nonce: String,
    pub records: Vec<Record>,
    pub addresses: Vec<(PublicKey, String)>,
}

impl PairRequest {
    pub fn to_text(&self) -> String {
        format!(
            "{REQUEST_PREFIX}{}",
            B64.encode(serde_json::to_vec(self).expect("serializable"))
        )
    }
    pub fn from_text(s: &str) -> Result<PairRequest, MeshError> {
        decode(s, REQUEST_PREFIX, "pairing request (from `potluck pair`)")
    }
}

impl PairReply {
    pub fn to_text(&self) -> String {
        format!(
            "{REPLY_PREFIX}{}",
            B64.encode(serde_json::to_vec(self).expect("serializable"))
        )
    }
    pub fn from_text(s: &str) -> Result<PairReply, MeshError> {
        decode(s, REPLY_PREFIX, "pairing reply (from `potluck admit`)")
    }
}

fn decode<T: serde::de::DeserializeOwned>(
    s: &str,
    prefix: &str,
    what: &str,
) -> Result<T, MeshError> {
    let s: String = s.chars().filter(|c| !c.is_whitespace()).collect();
    let body = s
        .strip_prefix(prefix)
        .ok_or_else(|| MeshError::Pairing(format!("not a {what}")))?;
    let bytes = B64
        .decode(body)
        .map_err(|_| MeshError::Pairing(format!("damaged {what}: was all of it pasted?")))?;
    serde_json::from_slice(&bytes).map_err(|e| MeshError::Pairing(format!("damaged {what}: {e}")))
}

/// Node names: 1 to 64 of `A-Z a-z 0-9 . _ -`, not starting with `-`: job and workspace ids
/// start with the node name, and `potluck wait -box-1` would read such an id as options.
pub fn valid_name(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= 64
        && !name.starts_with('-')
        && name
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '-'))
}

/// A random nonce, 16 bytes as base64url.
pub fn nonce() -> String {
    use ring::rand::SecureRandom;
    let mut b = [0u8; 16];
    ring::rand::SystemRandom::new()
        .fill(&mut b)
        .expect("system random");
    B64.encode(b)
}

#[cfg(test)]
mod tests {
    use super::*;

    struct M {
        id: Identity,
        name: &'static str,
        _dir: tempfile::TempDir,
    }

    fn machine(name: &'static str) -> M {
        let dir = tempfile::tempdir().unwrap();
        M {
            id: Identity::load_or_create(dir.path()).unwrap(),
            name,
            _dir: dir,
        }
    }

    fn admit(by: &M, who: &M, clock: u64) -> Record {
        admit_as(by, who.name, who.id.public(), clock)
    }

    fn admit_as(by: &M, name: &str, key: PublicKey, clock: u64) -> Record {
        Record::Admission(Admission::sign(
            &by.id, by.name, name, key, "h:1", "n", clock,
        ))
    }

    /// A removal of `who` keeping every record of `who`'s that `t` holds.
    fn remove(t: &Trust, by: &M, who: &M, clock: u64) -> Record {
        let kept = t.signed_by(&who.id.public());
        remove_keeping(by, who, kept, clock)
    }

    fn remove_keeping(by: &M, who: &M, kept: Vec<RecordId>, clock: u64) -> Record {
        Record::Removal(Removal::sign(
            &by.id,
            by.name,
            who.name,
            who.id.public(),
            kept,
            clock,
        ))
    }

    fn names(v: &View) -> Vec<String> {
        let mut n: Vec<String> = v.members.values().map(|a| a.node.clone()).collect();
        n.sort();
        n
    }

    /// Every order of `records` gives the same view (records are listed in a few orders).
    fn same_in_any_order(cluster: &PublicKey, records: &[Record]) -> View {
        let view = View::compute(cluster, records);
        let mut reversed = records.to_vec();
        reversed.reverse();
        assert_eq!(View::compute(cluster, &reversed), view);
        let mut rotated = records.to_vec();
        for _ in 0..records.len() {
            rotated.rotate_left(1);
            assert_eq!(View::compute(cluster, &rotated), view);
        }
        view
    }

    #[test]
    fn chain_of_trust_and_tampering() {
        let (a, b, c) = (machine("a"), machine("b"), machine("c"));
        let mut t = Trust::found(&a.id, "a", "h:1");
        t.merge(vec![admit(&a, &b, 1), admit(&b, &c, 2)]);
        assert_eq!(names(&t.view), ["a", "b", "c"]);

        // A record whose signature doesn't match its contents is dropped.
        let d = machine("d");
        let Record::Admission(mut forged) = admit(&b, &d, 3) else {
            unreachable!()
        };
        forged.node = "e".into();
        t.merge(vec![Record::Admission(forged)]);
        assert_eq!(names(&t.view), ["a", "b", "c"]);

        // A stranger's admissions, of itself or of others, are not even kept.
        let s = machine("s");
        let n = t.records().len();
        t.merge(vec![admit(&s, &d, 4), admit(&s, &s, 5)]);
        assert_eq!(names(&t.view), ["a", "b", "c"]);
        assert_eq!(t.records().len(), n);

        // An admission must come after its signer's (a higher clock).
        t.merge(vec![admit(&c, &d, 2)]);
        assert_eq!(names(&t.view), ["a", "b", "c"]);
        t.merge(vec![admit(&c, &d, 3)]);
        assert_eq!(names(&t.view), ["a", "b", "c", "d"]);
    }

    #[test]
    fn a_removal_keeps_the_records_it_lists_and_voids_the_rest() {
        let (a, b, c, x, y) = (
            machine("a"),
            machine("b"),
            machine("c"),
            machine("x"),
            machine("y"),
        );
        let mut t = Trust::found(&a.id, "a", "h:1");
        t.merge(vec![admit(&a, &b, 1), admit(&b, &c, 2)]);
        // a removes b; b's admission of c is on the list.
        let removal = remove(&t, &a, &b, 10);
        // b, removed, signs an admission dated before its removal; x then admits y.
        let backdated = admit(&b, &x, 5);
        let chained = admit(&x, &y, 6);
        let changes = t.merge(vec![removal, backdated, chained]);
        assert_eq!(names(&t.view), ["a", "c"]);
        assert_eq!(changes.left.len(), 1);
        assert!(t.view.removed.contains_key(&b.id.public()));
        // An admission b signs after its removal counts for nothing either.
        t.merge(vec![admit(&b, &x, 20)]);
        assert_eq!(names(&t.view), ["a", "c"]);
        // And b can't be admitted again.
        t.merge(vec![admit(&a, &b, 30)]);
        assert_eq!(names(&t.view), ["a", "c"]);
        same_in_any_order(&t.cluster, t.records());
    }

    #[test]
    fn a_record_the_remover_had_not_seen_is_lost() {
        // b admits x while a, not knowing, removes b: x is dropped, and says why.
        let (a, b, x) = (machine("a"), machine("b"), machine("x"));
        let mut t = Trust::found(&a.id, "a", "h:1");
        t.merge(vec![admit(&a, &b, 1)]);
        let removal = remove(&t, &a, &b, 2);
        let b_admits_x = admit(&b, &x, 2);
        let mut all = t.records().to_vec();
        all.extend([b_admits_x.clone(), removal.clone()]);
        t.merge(vec![b_admits_x.clone(), removal]);
        assert_eq!(names(&t.view), ["a"]);
        // It is void, so the store doesn't keep it.
        assert!(!t.records().contains(&b_admits_x));
        let why = Eval::run(&t.cluster, &all, true, MAX_MEMBERS).why_not_member(&x.id.public());
        assert!(why.contains("b admitted it"), "{why}");
        assert!(t.why_not_member(&b.id.public()).contains("removed by a"));
    }

    #[test]
    fn removing_the_founder_keeps_the_rest() {
        let (a, b, c) = (machine("a"), machine("b"), machine("c"));
        let mut t = Trust::found(&a.id, "a", "h:1");
        t.merge(vec![admit(&a, &b, 1), admit(&a, &c, 2)]);
        let removal = remove(&t, &b, &a, 3);
        t.merge(vec![removal]);
        assert_eq!(names(&t.view), ["b", "c"]);
        // c can still admit.
        let d = machine("d");
        t.merge(vec![admit(&c, &d, 4)]);
        assert_eq!(names(&t.view), ["b", "c", "d"]);
    }

    #[test]
    fn a_removal_that_voids_its_own_signer_does_not_count() {
        // c removes a (who admitted it) without listing its own admission.
        let (a, b, c) = (machine("a"), machine("b"), machine("c"));
        let mut t = Trust::found(&a.id, "a", "h:1");
        t.merge(vec![admit(&a, &b, 1), admit(&a, &c, 2)]);
        t.merge(vec![remove_keeping(&c, &a, Vec::new(), 3)]);
        assert_eq!(names(&t.view), ["a", "b", "c"]);
    }

    // Audit repro: a removed member's backdated removal of its remover.
    #[test]
    fn a_removed_member_cannot_undo_its_removal_with_a_backdated_removal() {
        let (a, b, c) = (machine("a"), machine("b"), machine("c"));
        let mut t = Trust::found(&a.id, "a", "h:1");
        t.merge(vec![admit(&a, &b, 1), admit(&a, &c, 2)]);
        let b_knew = t.signed_by(&a.id.public());
        let removal = remove(&t, &a, &b, 10);
        t.merge(vec![removal]);
        assert_eq!(names(&t.view), ["a", "c"]);
        // b, after its removal, signs a removal of a with an early clock, keeping what it knew of
        // a's records but not a's removal of it.
        let undo = remove_keeping(&b, &a, b_knew, 3);
        t.merge(vec![undo]);
        assert_eq!(names(&t.view), ["a", "c"]);
        assert!(!t.view.removed.contains_key(&a.id.public()));
        same_in_any_order(&t.cluster, t.records());
    }

    // Audit repro, held back: b signed a removal of a honestly while a member, and releases it
    // after a removed b.
    #[test]
    fn a_removed_member_cannot_undo_its_removal_with_a_held_back_removal() {
        let (a, b, c) = (machine("a"), machine("b"), machine("c"));
        let mut t = Trust::found(&a.id, "a", "h:1");
        t.merge(vec![admit(&a, &b, 1), admit(&a, &c, 2)]);
        let held_back = remove(&t, &b, &a, 3);
        let removal = remove(&t, &a, &b, 4);
        t.merge(vec![removal]);
        t.merge(vec![held_back]);
        assert_eq!(names(&t.view), ["a", "c"]);
        same_in_any_order(&t.cluster, t.records());
    }

    // Audit repro: a removed member's backdated admission under a member's name.
    #[test]
    fn a_removed_member_cannot_squat_a_name() {
        let (a, b, c, x) = (machine("a"), machine("b"), machine("c"), machine("x"));
        let mut t = Trust::found(&a.id, "a", "h:1");
        t.merge(vec![admit(&a, &b, 1), admit(&a, &c, 3)]);
        let removal = remove(&t, &a, &b, 10);
        t.merge(vec![removal]);
        assert_eq!(names(&t.view), ["a", "c"]);
        // b, removed, admits a key of its own under the name "c", dated before c's admission.
        t.merge(vec![admit_as(&b, "c", x.id.public(), 2)]);
        assert_eq!(names(&t.view), ["a", "c"]);
        assert_eq!(t.view.by_name("c").unwrap().key, c.id.public());
        // The same squat arriving before c's real admission doesn't block it either.
        let mut u = Trust::found(&a.id, "a", "h:1");
        u.merge(vec![admit(&a, &b, 1)]);
        let removal = remove(&u, &a, &b, 10);
        u.merge(vec![admit_as(&b, "c", x.id.public(), 2)]);
        u.merge(vec![removal]);
        u.merge(vec![admit(&a, &c, 11)]);
        assert_eq!(names(&u.view), ["a", "c"]);
        assert_eq!(u.view.by_name("c").unwrap().key, c.id.public());
    }

    #[test]
    fn mutual_removals_go_to_the_member_admitted_first() {
        // b and c remove each other, neither knowing of the other's removal. b was admitted
        // first, so its removal counts and c's is void, whatever their clocks.
        let (a, b, c) = (machine("a"), machine("b"), machine("c"));
        let mut t = Trust::found(&a.id, "a", "h:1");
        t.merge(vec![admit(&a, &b, 1), admit(&a, &c, 2)]);
        let b_removes_c = remove(&t, &b, &c, 50);
        let c_removes_b = remove(&t, &c, &b, 3);
        t.merge(vec![c_removes_b, b_removes_c]);
        assert_eq!(names(&t.view), ["a", "b"]);
        same_in_any_order(&t.cluster, t.records());

        // The founder wins against anyone.
        let mut u = Trust::found(&a.id, "a", "h:1");
        u.merge(vec![admit(&a, &b, 1)]);
        let a_removes_b = remove(&u, &a, &b, 9);
        let b_removes_a = remove(&u, &b, &a, 2);
        u.merge(vec![b_removes_a, a_removes_b]);
        assert_eq!(names(&u.view), ["a"]);
    }

    #[test]
    fn a_cycle_of_removals_goes_to_the_member_admitted_first() {
        // b removes c, c removes d, d removes b, none knowing of the others. b ranks first: its
        // removal of c counts; d's removal of b conflicts with it and c's is c's own, voided.
        let (a, b, c, d) = (machine("a"), machine("b"), machine("c"), machine("d"));
        let mut t = Trust::found(&a.id, "a", "h:1");
        t.merge(vec![admit(&a, &b, 1), admit(&a, &c, 2), admit(&a, &d, 3)]);
        let cycle = vec![
            remove(&t, &b, &c, 4),
            remove(&t, &c, &d, 4),
            remove(&t, &d, &b, 4),
        ];
        t.merge(cycle);
        assert_eq!(names(&t.view), ["a", "b", "d"]);
        same_in_any_order(&t.cluster, t.records());
    }

    #[test]
    fn names_are_unique_and_a_name_frees_up_after_a_removal() {
        let (a, b, b2, b3) = (machine("a"), machine("b"), machine("b"), machine("b"));
        let mut t = Trust::found(&a.id, "a", "h:1");
        let d0 = t.digest();
        // Two admissions under one name at once: the first (by clock) holds it.
        t.merge(vec![admit(&a, &b2, 2), admit(&a, &b, 1)]);
        assert_eq!(names(&t.view), ["a", "b"]);
        assert_eq!(t.view.by_name("b").unwrap().key, b.id.public());
        let why = t.why_not_member(&b2.id.public());
        assert!(why.contains("name b"), "{why}");
        // Once b is removed, a later admission may take the name; the loser stays out.
        let removal = remove(&t, &a, &b, 3);
        t.merge(vec![removal, admit(&a, &b3, 4)]);
        assert_eq!(names(&t.view), ["a", "b"]);
        assert_eq!(t.view.by_name("b").unwrap().key, b3.id.public());
        assert!(!t.view.members.contains_key(&b2.id.public()));
        // The digest doesn't depend on order.
        assert_ne!(t.digest(), d0);
        let mut u = Trust::found(&a.id, "a", "h:1");
        u.cluster = t.cluster;
        u.records = t.records.clone();
        u.records.reverse();
        u.recompute();
        assert_eq!(u.digest(), t.digest());
        same_in_any_order(&t.cluster, t.records());
    }

    #[test]
    fn pairing_texts_round_trip() {
        let a = machine("a");
        let req = PairRequest {
            v: 1,
            name: "a".into(),
            key: a.id.public(),
            address: "a.lan:7465".into(),
            version: "0.1.0".into(),
            nonce: nonce(),
        };
        let text = req.to_text();
        assert!(
            text.chars()
                .all(|c| c.is_ascii_alphanumeric() || "-_.".contains(c))
        );
        // Line breaks from a terminal are ignored.
        let wrapped = format!("{}\n  {}", &text[..30], &text[30..]);
        assert_eq!(PairRequest::from_text(&wrapped).unwrap(), req);
        assert!(matches!(
            PairReply::from_text(&text),
            Err(MeshError::Pairing(_))
        ));
        assert!(matches!(
            PairRequest::from_text(&text[..text.len() - 5]),
            Err(MeshError::Pairing(_))
        ));
        assert!(valid_name("msi-laptop") && valid_name("studio.lan") && !valid_name("a b"));
        assert!(valid_name("box-") && !valid_name("-box") && !valid_name("-"));
    }

    #[test]
    fn the_view_does_not_depend_on_record_order() {
        // The founder lost its records (not its key) and founded again under another name: two
        // founding records for one cluster. Members that hold both must agree on the founder.
        let (a, b) = (machine("a"), machine("b"));
        let first = Trust::found(&a.id, "a", "h:1");
        let again = Record::Admission(Admission::sign(
            &a.id,
            "a2",
            "a2",
            a.id.public(),
            "h:1",
            "",
            5,
        ));
        let records = vec![first.records()[0].clone(), admit(&a, &b, 1), again];
        same_in_any_order(&a.id.public(), &records);
    }

    // Audit (round 7 trust): a pairing reply carries the members' proof, not every record that
    // counts. For stores of random records (removals that cross, names taken, room for a few),
    // the proof implies the same members, removes only keys the store has removed, and holds
    // only records that count.
    #[test]
    fn the_members_proof_implies_the_same_members() {
        let keys: Vec<M> = ["a", "b", "c", "d", "e", "f", "g"]
            .into_iter()
            .map(machine)
            .collect();
        let mut seed: u64 = 0x5851_f42d_4c95_7f2d;
        let mut next = |n: u64| {
            seed ^= seed << 13;
            seed ^= seed >> 7;
            seed ^= seed << 17;
            seed % n
        };
        let node_names = ["a", "b", "c", "d", "x"];
        for case in 0..2_000 {
            let limit = [2, 3, 4, MAX_MEMBERS][next(4) as usize];
            let mut t = store(&keys[0], &keys[next(7) as usize], limit);
            for _ in 0..(3 + next(15)) {
                let signer = &keys[next(6) as usize];
                let subject = &keys[1 + next(6) as usize];
                let clock = 1 + next(8);
                let r = if next(10) < 6 {
                    admit_as(
                        signer,
                        node_names[next(5) as usize],
                        subject.id.public(),
                        clock,
                    )
                } else {
                    let kept = t
                        .signed_by(&subject.id.public())
                        .into_iter()
                        .filter(|_| next(3) > 0)
                        .collect();
                    remove_keeping(signer, subject, kept, clock)
                };
                t.merge(vec![r]);
            }
            let proof = t.members_proof();
            let v = View::of_verified(&t.cluster, &proof, limit);
            assert_eq!(v.members, t.view.members, "case {case}");
            assert!(
                v.removed.keys().all(|k| t.view.removed.contains_key(k)),
                "case {case}"
            );
            assert!(proof.iter().all(|r| t.view.valid.contains(&r.id())));
        }
        // A cluster that admitted and removed many machines: the proof is the members'.
        let (a, b) = (machine("a"), machine("b"));
        let mut t = Trust::found(&a.id, "a", "h:1");
        t.merge(vec![admit(&a, &b, 1)]);
        t.merge(flood(&a, 2_000, 3));
        assert_eq!(names(&t.view), ["a", "b"]);
        assert_eq!(t.members_proof().len(), 2);
    }

    #[test]
    fn a_record_at_the_end_of_logical_time_does_not_break_new_records() {
        let (a, b, s) = (machine("a"), machine("b"), machine("s"));
        let mut t = Trust::found(&a.id, "a", "h:1");
        // A stranger's isn't kept; a void one doesn't count.
        t.merge(vec![admit(&s, &s, u64::MAX)]);
        assert_eq!(t.next_clock(), 1);
        // A member's saturates rather than overflows.
        t.merge(vec![admit(&a, &b, u64::MAX)]);
        assert_eq!(names(&t.view), ["a", "b"]);
        assert_eq!(t.next_clock(), u64::MAX);
    }

    /// A store founded by `a`, held by `me`, with room for `limit` members.
    fn store(a: &M, me: &M, limit: usize) -> Trust {
        let mut t = Trust::found(&a.id, a.name, "h:1");
        t.me = Some(me.id.public());
        t.limit = limit;
        t.recompute();
        t
    }

    /// What a member `b` can sign in bulk: admissions of new keys, each removed after (keeping
    /// nothing). `n` records in all.
    fn flood(b: &M, n: usize, tag: u8) -> Vec<Record> {
        let mut out = Vec::new();
        let mut i = 0u32;
        while out.len() < n {
            let mut k = [0u8; 32];
            k[0] = tag;
            k[1..5].copy_from_slice(&i.to_le_bytes());
            let name = format!("x{tag}-{i}");
            let clock = 100 + 2 * u64::from(i);
            out.push(admit_as(b, &name, PublicKey(k), clock));
            if out.len() < n {
                out.push(Record::Removal(Removal::sign(
                    &b.id,
                    b.name,
                    &name,
                    PublicKey(k),
                    Vec::new(),
                    clock + 1,
                )));
            }
            i += 1;
        }
        out
    }

    // Owner's design (round 5): a member away when b was removed hears from b first. b, still a
    // member there, admits keys and removes them: records that count there, kept with no cap.
    // The removal of b, which kept none of them, voids them: turned away when b sends them again
    // (without their signatures being checked), or with the removal, and dropped where held when
    // the store is next loaded (round 9: a merge drops no record held, see `Trust::prune`).
    #[test]
    fn records_a_removal_voids_are_dropped_where_held_and_refused_after() {
        let (a, b, c) = (machine("a"), machine("b"), machine("c"));
        let mut base = Trust::found(&a.id, "a", "h:1");
        base.merge(vec![admit(&a, &b, 1), admit(&a, &c, 2)]);
        let removal = remove(&base, &a, &b, 3);
        let junk = flood(&b, 300, 0);
        for same_batch in [false, true] {
            let mut t = store(&a, &c, MAX_MEMBERS);
            t.merge(base.records().to_vec());
            let held = t.records().len();
            let changes = if same_batch {
                let mut both = junk.clone();
                both.push(removal.clone());
                t.merge(both)
            } else {
                t.merge(junk.clone());
                assert_eq!(t.records().len(), held + junk.len());
                assert_eq!(names(&t.view), ["a", "b", "c"]);
                t.merge(vec![removal.clone()])
            };
            assert!(changes.stored);
            assert_eq!(names(&t.view), ["a", "c"]);
            assert!(t.view.removed.contains_key(&b.id.public()));
            if same_batch {
                assert_eq!(t.records().len(), held + 1, "the junk is turned away");
            } else {
                assert_eq!(t.records().len(), held + junk.len() + 1, "held till loaded");
                // Loaded again (a daemon start), the store drops it.
                let dir = tempfile::tempdir().unwrap();
                t.path = Some(dir.path().join(STATE_FILE));
                t.save().unwrap();
                t = Trust::load_or_found(dir.path(), &c.id, "c", "h:1").unwrap();
                assert_eq!(names(&t.view), ["a", "c"]);
                assert_eq!(t.records().len(), held + 1, "the junk is dropped");
            }
            assert_eq!(t.merge(junk.clone()), Changes::default());
            assert_eq!(t.records().len(), held + 1);
            assert!(
                t.screen(with_ids(junk.clone())).is_empty(),
                "turned away for now"
            );
        }
    }

    // Owner's design (round 5): a removal voids every record its key signed that the remover
    // hadn't seen, whatever `at` and `clock` the record claims.
    #[test]
    fn a_removed_key_cannot_backdate_records() {
        let (a, b, c, z) = (machine("a"), machine("b"), machine("c"), machine("z"));
        let mut t = Trust::found(&a.id, "a", "h:1");
        t.merge(vec![admit(&a, &b, 1), admit(&a, &c, 2)]);
        let c_records = t.signed_by(&c.id.public());
        let removal = remove(&t, &a, &b, 10);
        t.merge(vec![removal]);
        // After its removal, b signs records dated before it: the earliest `at`, a low `clock`.
        let Record::Admission(mut adm) = admit(&b, &z, 0) else {
            unreachable!()
        };
        adm.at = 1;
        adm.sig = b.id.sign(&adm.signed_bytes());
        let Record::Removal(mut rm) = remove_keeping(&b, &c, c_records, 0) else {
            unreachable!()
        };
        rm.at = 1;
        rm.sig = b.id.sign(&rm.signed_bytes());
        let backdated = vec![Record::Admission(adm), Record::Removal(rm)];
        assert!(backdated.iter().all(Record::verify));
        // Refused on arrival, and remembered so.
        let held = t.records().to_vec();
        assert_eq!(t.merge(backdated.clone()), Changes::default());
        assert_eq!(t.records(), held);
        assert!(t.screen(with_ids(backdated.clone())).is_empty());
        assert_eq!(names(&t.view), ["a", "c"]);
        // And they count nowhere, whatever the order of the records.
        let mut all = t.records().to_vec();
        all.extend(backdated.iter().cloned());
        let v = same_in_any_order(&t.cluster, &all);
        assert_eq!(names(&v), ["a", "c"]);
        assert!(backdated.iter().all(|r| !v.valid.contains(&r.id())));
    }

    // Owner's design (round 5): room for `limit` members. An admission into a full cluster
    // counts nowhere (it is kept and passed on all the same: it isn't void); a removal before it
    // (by clock) makes room.
    #[test]
    fn the_cluster_has_room_for_its_limit_and_a_removal_makes_room() {
        let (a, b, c, d) = (machine("a"), machine("b"), machine("c"), machine("d"));
        let mut t = store(&a, &a, 3);
        t.merge(vec![admit(&a, &b, 1), admit(&a, &c, 2)]);
        assert_eq!(names(&t.view), ["a", "b", "c"]);
        let full = admit(&b, &d, 3);
        let mut with = t.records().to_vec();
        with.push(full.clone());
        assert!(
            Eval::run(&t.cluster, &with, false, 3)
                .why_not_member(&d.id.public())
                .contains("3 members, the most it can have")
        );
        assert!(t.merge(vec![full.clone()]).is_empty());
        assert!(t.records().contains(&full));
        assert!(!t.view.valid.contains(&full.id()));
        assert_eq!(names(&t.view), ["a", "b", "c"]);
        // A removal at the same clock doesn't make room for it; one before it does.
        let removal = remove(&t, &a, &c, 3);
        t.merge(vec![removal]);
        t.merge(vec![full.clone()]);
        assert_eq!(names(&t.view), ["a", "b"]);
        assert!(!t.view.valid.contains(&full.id()));
        let after = admit(&b, &d, 4);
        t.merge(vec![after.clone()]);
        assert_eq!(names(&t.view), ["a", "b", "d"]);
        assert!(t.records().contains(&after));
        same_in_any_order(&t.cluster, t.records());
    }

    // Owner's design (round 5): two members admit a machine each at once into the last place.
    // Each takes its own; once they exchange records the admission first by (clock, id) counts
    // on both, and the other counts nowhere (both keep it: it isn't void).
    #[test]
    fn two_admissions_at_once_into_the_last_place_agree_on_one() {
        let (a, b, c, p, q) = (
            machine("a"),
            machine("b"),
            machine("c"),
            machine("p"),
            machine("q"),
        );
        let mut base = store(&a, &a, 4);
        base.merge(vec![admit(&a, &b, 1), admit(&a, &c, 2)]);
        let b_admits_p = admit(&b, &p, 3);
        let c_admits_q = admit(&c, &q, 3);
        let (mut x, mut y) = (store(&a, &b, 4), store(&a, &c, 4));
        x.merge(base.records().to_vec());
        y.merge(base.records().to_vec());
        x.merge(vec![b_admits_p.clone()]);
        y.merge(vec![c_admits_q.clone()]);
        assert_eq!(names(&x.view), ["a", "b", "c", "p"]);
        assert_eq!(names(&y.view), ["a", "b", "c", "q"]);
        // x syncs with y, then y with x.
        y.merge(x.records().to_vec());
        x.merge(y.records().to_vec());
        assert_eq!(x.view.members, y.view.members);
        assert_eq!(x.view.removed, y.view.removed);
        assert_eq!(x.view.valid, y.view.valid);
        let (winner, loser, holder) = match b_admits_p.id() < c_admits_q.id() {
            true => ("p", &c_admits_q, &y),
            false => ("q", &b_admits_p, &x),
        };
        let mut want = vec!["a", "b", "c", winner];
        want.sort();
        assert_eq!(names(&x.view), want);
        assert!(!x.view.valid.contains(&loser.id()));
        assert!(holder.records().contains(loser));
        assert_eq!(x.digest(), y.digest());
        let mut all = x.records().to_vec();
        all.push(loser.clone());
        all.dedup();
        assert_eq!(names(&View::of_verified(&x.cluster, &all, 4)), want);
    }

    // Audit repro (round 5 trust, the s1 residual): b, removed by a, removes c (keeping none of
    // c's records) before c's admission of z reaches a member away. There, c's admission of z
    // is void while b's removal of c stands, and at the cap it was dropped for good: once a's
    // removal of b arrived, c was a member again but z was not. Now c keeps its own record, and
    // passes it on again.
    #[test]
    fn an_admission_a_void_removal_voided_comes_back_from_its_signer() {
        let (a, b, c, z) = (machine("a"), machine("b"), machine("c"), machine("z"));
        let mut base = Trust::found(&a.id, "a", "h:1");
        base.merge(vec![admit(&a, &b, 1), admit(&a, &c, 2)]);
        let a_removes_b = remove(&base, &a, &b, 3);
        let c_admits_z = admit(&c, &z, 3);
        let b_removes_c = remove_keeping(&b, &c, Vec::new(), 500);
        let junk = flood(&b, 300, 4);
        // c's store, and the store of a member away (it is not c), which hears of b's removal
        // of c first.
        let (mut at_c, mut away) = (store(&a, &c, MAX_MEMBERS), store(&a, &a, MAX_MEMBERS));
        for t in [&mut at_c, &mut away] {
            t.records = base.records().to_vec();
            t.recompute();
            t.merge(junk.clone());
        }
        at_c.merge(vec![c_admits_z.clone()]);
        at_c.merge(vec![b_removes_c.clone()]);
        away.merge(vec![b_removes_c.clone()]);
        away.merge(vec![c_admits_z.clone()]);
        for t in [&at_c, &away] {
            assert!(!t.view.members.contains_key(&z.id.public()));
        }
        assert!(at_c.records().contains(&c_admits_z));
        assert!(!away.records().contains(&c_admits_z), "void on arrival");
        for t in [&mut at_c, &mut away] {
            t.merge(vec![a_removes_b.clone()]);
        }
        assert_eq!(names(&at_c.view), ["a", "c", "z"]);
        assert_eq!(names(&away.view), ["a", "c"]);
        // c passes its records on.
        away.merge(at_c.records().to_vec());
        assert_eq!(names(&away.view), ["a", "c", "z"]);
        assert_eq!(away.digest(), at_c.digest());
    }

    /// What a pairing reply or a refusal carries: the records that count, a few, not the
    /// records the removal voided, and they imply the same members, removals and records that
    /// count as the whole store.
    #[test]
    fn the_records_that_count_imply_the_same_view() {
        let (a, b, c, d) = (machine("a"), machine("b"), machine("c"), machine("d"));
        let mut t = Trust::found(&a.id, "a", "h:1");
        t.merge(vec![admit(&a, &b, 1), admit(&a, &c, 2), admit(&a, &d, 3)]);
        let base = t.records().to_vec();
        let mut probe = Trust::found(&a.id, "a", "h:1");
        probe.merge(base);
        // Removals that cross (c's, the later admitted, is void), and d's flood before its own.
        let b_removes_c = remove(&probe, &b, &c, 4);
        let c_removes_b = remove(&probe, &c, &b, 4);
        t.merge(flood(&d, 200, 1));
        let a_removes_d = remove(&t, &a, &d, 500);
        t.merge(vec![b_removes_c, c_removes_b, a_removes_d]);
        assert_eq!(names(&t.view), ["a", "b"]);
        let counting = t.counting();
        assert_eq!(counting.len(), t.view.valid.len());
        assert!(counting.len() > 200, "{} records", counting.len());
        let v = View::compute(&t.cluster, &counting);
        assert_eq!(v.members, t.view.members);
        assert_eq!(v.removed, t.view.removed);
        assert_eq!(v.valid, t.view.valid);
    }

    #[test]
    fn a_record_refused_is_taken_once_it_counts() {
        // b's admission of x arrives while b is removed (it counts nowhere: refused). Once b's
        // removal is void, b's admission of x counts and is taken, though it was refused before.
        let (a, b, c, x) = (machine("a"), machine("b"), machine("c"), machine("x"));
        let mut t = Trust::found(&a.id, "a", "h:1");
        t.merge(vec![admit(&a, &b, 1), admit(&a, &c, 2)]);
        // c removes b, not knowing of b's admission of x; b, not knowing of that, removes c.
        let c_removes_b = remove(&t, &c, &b, 5);
        let b_admits_x = admit(&b, &x, 5);
        t.merge(vec![c_removes_b]);
        t.merge(vec![b_admits_x.clone()]);
        assert!(!t.records().contains(&b_admits_x));
        assert!(t.screen(with_ids(vec![b_admits_x.clone()])).is_empty());
        assert_eq!(names(&t.view), ["a", "c"]);
        // b's removal of c, admitted before it, wins: b is back, and its admission of x counts.
        let b_removes_c = remove_keeping(&b, &c, Vec::new(), 6);
        t.merge(vec![b_removes_c]);
        assert_eq!(names(&t.view), ["a", "b"]);
        t.merge(vec![b_admits_x.clone()]);
        assert!(t.records().contains(&b_admits_x));
        assert_eq!(names(&t.view), ["a", "b", "x"]);
    }

    // Audit repro (round 7 trust): a record turned away was remembered on its own, and filtered
    // out of every batch after, even one that brought what makes it count. s admits k while q
    // removes s (not knowing of it) and k removes q (not knowing of q's): k ranks before q, so
    // k's removal counts and s's admission of k with it. t, holding q's removal already, turned
    // s's admission away when it was announced, couldn't take k's removal on its own (k not
    // admitted), and then refused both together from every member for good: s stayed removed
    // and q a member there while everywhere else it was the other way round.
    #[test]
    fn records_turned_away_one_at_a_time_are_taken_together() {
        let (a, s, q, k) = (machine("a"), machine("s"), machine("q"), machine("k"));
        let s_admitted = admit(&a, &s, 1);
        let s_admits_k = admit(&s, &k, 2);
        let q_admitted = admit(&a, &q, 3);
        let q_removes_s = remove_keeping(&q, &s, Vec::new(), 4);
        let k_removes_q = remove_keeping(&k, &q, Vec::new(), 3);
        let mut t = Trust::found(&a.id, "a", "h:1");
        let all = vec![
            t.records()[0].clone(),
            s_admitted.clone(),
            s_admits_k.clone(),
            q_admitted.clone(),
            q_removes_s.clone(),
            k_removes_q.clone(),
        ];
        let want = same_in_any_order(&a.id.public(), &all);
        assert_eq!(names(&want), ["a", "k", "s"]);
        t.merge(vec![s_admitted, q_admitted, q_removes_s]);
        assert_eq!(names(&t.view), ["a", "q"]);
        // The announcements, one at a time: neither is taken.
        t.merge(vec![s_admits_k]);
        t.merge(vec![k_removes_q]);
        assert_eq!(names(&t.view), ["a", "q"]);
        // A member's records, all of them.
        t.merge(all);
        assert_eq!(names(&t.view), ["a", "k", "s"]);
        assert_eq!(t.view, want);
    }

    // Audit repro (round 4 security): every record arriving had its signature checked first,
    // under the mesh's lock on the store: a member's 64 MiB of junk held it for about 7 s
    // (20,000 junk records: 590 ms there; now 1.6 ms there, the rest without it).
    #[test]
    fn records_are_screened_before_their_signatures_are_checked() {
        let (a, b, s) = (machine("a"), machine("b"), machine("s"));
        let mut t = Trust::found(&a.id, "a", "h:1");
        t.merge(vec![admit(&a, &b, 1)]);
        // A stranger's records go no further.
        let stranger: Vec<Record> = (0..300)
            .map(|i| admit_as(&s, &format!("x{i}"), s.id.public(), 5))
            .collect();
        assert!(t.screen(with_ids(stranger)).is_empty());
        // Junk naming a member as its signer: its signatures are checked without the store.
        let Record::Admission(one) = admit(&s, &s, 5) else {
            unreachable!()
        };
        let forged: Vec<Record> = (0..300)
            .map(|i| {
                let mut x = one.clone();
                x.by_key = b.id.public();
                x.nonce = i.to_string();
                Record::Admission(x)
            })
            .collect();
        let screened = t.screen(with_ids(forged));
        assert_eq!(screened.len(), 300);
        assert!(Checked::new(screened).records.is_empty());
        // A chain that comes last link first is taken whole.
        let (c, d, e) = (machine("c"), machine("d"), machine("e"));
        let chain = vec![admit(&d, &e, 4), admit(&c, &d, 3), admit(&b, &c, 2)];
        let screened = t.screen(with_ids(chain));
        assert_eq!(screened.len(), 3);
        t.merge_checked(Checked::new(screened));
        assert_eq!(names(&t.view), ["a", "b", "c", "d", "e"]);
    }

    /// Records signed by `by` with `at` fixed, so the test doesn't depend on the clock.
    fn admit_at(by: &M, name: &str, key: PublicKey, clock: u64) -> Record {
        let Record::Admission(mut a) = admit_as(by, name, key, clock) else {
            unreachable!()
        };
        a.at = 0;
        a.sig = by.id.sign(&a.signed_bytes());
        Record::Admission(a)
    }

    /// Exchanges records along `edges` until a round changes nothing (at most 20 rounds): both
    /// ways, as members do (one sends its records, the other takes them and answers with its
    /// own), or only from the first store of each edge to the second. Returns whether it ended.
    fn exchange_until_quiet(stores: &mut [Trust], edges: &[(usize, usize)], both: bool) -> bool {
        for _ in 0..20 {
            let mut changed = false;
            for &(x, y) in edges {
                let sent = stores[x].records().to_vec();
                changed |= stores[y].merge(sent).stored;
                if both {
                    let back = stores[y].records().to_vec();
                    changed |= stores[x].merge(back).stored;
                }
            }
            if !changed {
                return true;
            }
        }
        false
    }

    /// Stores held by `mes`, founded by `a` with one founding record, with room for `limit`.
    fn stores<const N: usize>(a: &M, mes: [&M; N], limit: usize) -> [Trust; N] {
        let base = store(a, a, limit);
        mes.map(|me| {
            let mut t = base.clone();
            t.me = Some(me.id.public());
            t
        })
    }

    fn agree(stores: &[Trust]) -> bool {
        stores.windows(2).all(|w| {
            (&w[0].view.members, &w[0].view.removed, &w[0].view.valid)
                == (&w[1].view.members, &w[1].view.removed, &w[1].view.valid)
        })
    }

    // Audit repro (round 9 trust): a store dropped the records that counted nowhere for itself,
    // so a store that heard from another only through it never got them; it took others that a
    // record it dropped would have made count nowhere. 0 splits in 500,000 random cases when
    // every store exchanges with every other; in a chain, ring, star or random tree, about 1 in
    // 30,000 never stopped changing (the store in the middle flipped at every exchange), and in
    // a one-way ring 1 in 30,000 ended split. Now a store keeps every record that isn't void,
    // and a merge drops no record held.
    #[test]
    fn stores_that_hear_each_other_only_through_a_third_agree() {
        let (a, b, c, e, f, g) = (
            machine("a"),
            machine("b"),
            machine("c"),
            machine("e"),
            machine("f"),
            machine("g"),
        );
        // A chain b <-> g <-> a, as members exchange records. a admits g, e and c under one
        // name (clocks 3, 5, 6) and c removes g at clock 4, keeping nothing. With all four, g
        // keeps the name: c's removal would free it at 4 for e (5), not c (6), so c's admission
        // and with it c's removal count nowhere. Without e's admission, c's removal frees the
        // name for c and counts. g's store took c's records from b's store and counted them (g
        // removed), took e's admission from a's store and dropped c's records and e's admission
        // (none counted then), took c's records again, and so on at every exchange; b's store
        // never heard of e's admission and a's kept turning c's removal away: b said g was
        // removed and c was "x", a said g was "x".
        let r0 = admit_at(&a, "x", g.id.public(), 3);
        let r1 = admit_at(&a, "x", e.id.public(), 5);
        let r2 = admit_at(&a, "x", c.id.public(), 6);
        let Record::Removal(mut r3) = remove_keeping(&c, &g, Vec::new(), 4) else {
            unreachable!()
        };
        r3.at = 0;
        r3.sig = c.id.sign(&r3.signed_bytes());
        let r3 = Record::Removal(r3);
        let mut chain = stores(&a, [&b, &g, &a], MAX_MEMBERS);
        chain[0].merge(vec![r2.clone(), r3.clone()]);
        chain[1].merge(vec![r0.clone()]);
        chain[2].merge(vec![r1.clone()]);
        assert!(exchange_until_quiet(&mut chain, &[(0, 1), (1, 2)], true));
        assert!(agree(&chain));
        let all = vec![chain[2].records()[0].clone(), r0, r1, r2, r3];
        let want = same_in_any_order(&a.id.public(), &all);
        assert_eq!(chain[1].view.by_name("x").unwrap().key, g.id.public());
        assert_eq!(chain[1].view, want);

        // audit8-review's case: a ring where each store hears only the one before it (records
        // reach a store one way, as announcements do). Room for 2: a and f fill it. f admits b
        // (clock 5) and c (6); c removes f at 4, keeping both. With all four, b takes the place
        // f's removal would free, so c's admission and c's removal count nowhere; without f's
        // admission of b, c's removal counts. c's store took f's records but turned away the
        // admission of b (it counted nowhere there), so e's store, which hears only from c's,
        // never had it and ended with f removed while the others kept f.
        let r0 = admit_at(&a, "b", f.id.public(), 1);
        let r1 = admit_at(&f, "x", b.id.public(), 5);
        let r2 = admit_at(&f, "d", c.id.public(), 6);
        let kept = vec![r1.id(), r2.id()];
        let Record::Removal(mut r3) = remove_keeping(&c, &f, kept, 4) else {
            unreachable!()
        };
        r3.at = 0;
        r3.sig = c.id.sign(&r3.signed_bytes());
        let r3 = Record::Removal(r3);
        let mut ring = stores(&a, [&c, &e, &f], 2);
        ring[1].merge(vec![r0.clone(), r2.clone(), r3.clone()]);
        ring[2].merge(vec![r0.clone()]);
        ring[2].merge(vec![r1.clone()]);
        assert!(exchange_until_quiet(
            &mut ring,
            &[(0, 1), (1, 2), (2, 0)],
            false
        ));
        assert!(agree(&ring));
        assert!(ring[1].view.members.contains_key(&f.id.public()));
        assert!(!ring[1].view.removed.contains_key(&f.id.public()));
    }

    // Owner's design (round 5): a store drops records that count nowhere, but its own. Two
    // stores that take random records in random batches, then exchange records as heartbeats do
    // until neither changes, agree on the members, the removals and the records that count.
    #[test]
    fn stores_that_drop_records_agree_once_they_exchange_them() {
        let keys: Vec<M> = ["a", "b", "c", "d", "e", "f", "g"]
            .into_iter()
            .map(machine)
            .collect();
        let mut seed: u64 = 0x2545_f491_4f6c_dd1d;
        let mut next = |n: u64| {
            seed ^= seed << 13;
            seed ^= seed >> 7;
            seed ^= seed << 17;
            seed % n
        };
        let names = ["a", "b", "c", "d", "x"];
        for case in 0..1_500 {
            let limit = [2, 3, 4, MAX_MEMBERS][next(4) as usize];
            let mut records: Vec<Record> = Vec::new();
            for _ in 0..(3 + next(15)) {
                let signer = &keys[next(6) as usize];
                let subject = &keys[1 + next(6) as usize];
                let clock = 1 + next(8);
                if next(10) < 6 {
                    records.push(Record::Admission(Admission::sign(
                        &signer.id,
                        signer.name,
                        names[next(names.len() as u64) as usize],
                        subject.id.public(),
                        "h:1",
                        "",
                        clock,
                    )));
                } else {
                    let kept = records
                        .iter()
                        .filter(|r| r.signer() == subject.id.public() && next(3) > 0)
                        .map(Record::id)
                        .collect();
                    records.push(Record::Removal(Removal::sign(
                        &signer.id,
                        signer.name,
                        subject.name,
                        subject.id.public(),
                        kept,
                        clock,
                    )));
                }
            }
            let mut x = store(&keys[0], &keys[next(7) as usize], limit);
            let mut y = store(&keys[0], &keys[next(7) as usize], limit);
            y.records = x.records.clone();
            y.recompute();
            // Each record goes to x, y or both, in batches.
            let (mut to_x, mut to_y) = (Vec::new(), Vec::new());
            for r in &records {
                match next(3) {
                    0 => to_x.push(r.clone()),
                    1 => to_y.push(r.clone()),
                    _ => {
                        to_x.push(r.clone());
                        to_y.push(r.clone());
                    }
                }
            }
            for (t, mut rs) in [(&mut x, to_x), (&mut y, to_y)] {
                while !rs.is_empty() {
                    let n = 1 + next(rs.len() as u64) as usize;
                    t.merge(rs.drain(..n).collect());
                }
            }
            for _ in 0..10 {
                let to_y = y.merge(x.records().to_vec());
                let to_x = x.merge(y.records().to_vec());
                if !to_y.stored && !to_x.stored {
                    break;
                }
            }
            assert_eq!(
                (&x.view.members, &x.view.removed, &x.view.valid),
                (&y.view.members, &y.view.removed, &y.view.valid),
                "case {case}, limit {limit}"
            );
            assert!(x.view.members.len() <= limit);
        }
    }

    /// Labels `records` with the propagation of before round 4 or the one now, with room for
    /// `limit` members.
    fn labels(
        cluster: &PublicKey,
        records: &[Record],
        slow: bool,
        limit: usize,
    ) -> (Vec<Label>, View) {
        SLOW_PROPAGATE.with(|s| s.set(slow));
        let e = Eval::run(cluster, records, false, limit);
        SLOW_PROPAGATE.with(|s| s.set(false));
        let view = e.view();
        (e.lab, view)
    }

    // Audit repro (round 4 security): propagation decided one link of a chain of admissions per
    // pass over every record, so a view took time in the square of the chain's length: 2.8 s
    // for a chain of 4,000 (1,000: 0.18 s), and the mesh computes it under its lock on the
    // store at every change. Now one pass decides a chain; the labels must not change.
    #[test]
    fn a_chain_is_labelled_in_one_pass_as_one_link_at_a_time_did() {
        let keys: Vec<M> = ["a", "b", "c", "d", "e", "f", "g"]
            .into_iter()
            .map(machine)
            .collect();
        let cluster = keys[0].id.public();
        let mut seed: u64 = 0x9e37_79b9_7f4a_7c15;
        let mut next = |n: u64| {
            seed ^= seed << 13;
            seed ^= seed >> 7;
            seed ^= seed << 17;
            seed % n
        };
        let names = ["a", "b", "c", "d", "x"];
        for _ in 0..20_000 {
            let mut records = vec![Record::Admission(Admission {
                node: "a".into(),
                key: cluster,
                address: "h:1".into(),
                by: "a".into(),
                by_key: cluster,
                at: 0,
                clock: 0,
                nonce: String::new(),
                pool: None,
                sig: Signature([0; 64]),
            })];
            for _ in 0..(3 + next(20)) {
                let signer = &keys[next(6) as usize];
                let subject = &keys[1 + next(6) as usize];
                let clock = 1 + next(8);
                let at = next(1_000_000);
                if next(10) < 6 {
                    records.push(Record::Admission(Admission {
                        node: names[next(names.len() as u64) as usize].into(),
                        key: subject.id.public(),
                        address: "h:1".into(),
                        by: signer.name.into(),
                        by_key: signer.id.public(),
                        at,
                        clock,
                        nonce: String::new(),
                        pool: None,
                        sig: Signature([0; 64]),
                    }));
                } else {
                    let kept = records
                        .iter()
                        .filter(|r| r.signer() == subject.id.public() && next(3) > 0)
                        .map(Record::id)
                        .collect();
                    records.push(Record::Removal(Removal {
                        node: subject.name.into(),
                        key: subject.id.public(),
                        by: signer.name.into(),
                        by_key: signer.id.public(),
                        at,
                        clock,
                        kept,
                        by_user: false,
                        sig: Signature([0; 64]),
                    }));
                }
            }
            // Room for a few members, so that it decides some admissions, or for many.
            let limit = [2, 3, 4, MAX_MEMBERS][next(4) as usize];
            let (slow, slow_view) = labels(&cluster, &records, true, limit);
            let (fast, fast_view) = labels(&cluster, &records, false, limit);
            assert_eq!(slow, fast, "limit {limit}: {records:#?}");
            assert_eq!(slow_view, fast_view);
            assert!(fast_view.members.len() <= limit);
            // The void records count nowhere.
            let void = void_records(&cluster, &records, &fast_view, |_| false);
            assert!(void.iter().all(|id| !fast_view.valid.contains(id)));
        }
        // A chain of 1,500 admissions given in any order: one pass, not 1,500.
        let mut chain = vec![admit(&keys[0], &keys[1], 1)];
        let mut prev = keys[1].id.public();
        let scratch = tempfile::tempdir().unwrap();
        let mut signers = Vec::new();
        for i in 0..1_500u64 {
            let k = Identity::create(scratch.path()).unwrap();
            let by = signers.last().unwrap_or(&keys[1].id);
            chain.push(Record::Admission(Admission::sign(
                by,
                "p",
                &format!("c{i}"),
                k.public(),
                "h:1",
                "n",
                i + 2,
            )));
            prev = k.public();
            signers.push(k);
        }
        chain.reverse();
        let mut t = Trust::found(&keys[0].id, "a", "h:1");
        let screened = t.screen(with_ids(chain));
        let checked = Checked::new(screened);
        PASSES.with(|p| p.set(0));
        t.merge_checked(checked);
        assert!(t.view.members.contains_key(&prev));
        assert_eq!(t.view.members.len(), 1_502);
        let passes = PASSES.with(std::cell::Cell::get);
        assert!(passes <= 3, "{passes} passes");
    }

    // ---- Users (two tiers of authority) ----

    use super::super::user::UserKey;

    struct U {
        key: UserKey,
        name: &'static str,
        _dir: tempfile::TempDir,
    }

    fn user(name: &'static str) -> U {
        let dir = tempfile::tempdir().unwrap();
        let (key, _) = UserKey::load_or_create(&dir.path().join("user.key")).unwrap();
        U {
            key,
            name,
            _dir: dir,
        }
    }

    /// `u`'s first-user admission, co-signed by machine `m`.
    fn genesis(u: &U, m: &M, clock: u64) -> Record {
        let mut g = UserAdmission::genesis(&u.key, u.name, m.name, m.id.public(), clock);
        g.co_sign(&m.id).unwrap();
        Record::User(g)
    }

    /// `by`'s admission (v3) of machine `who` into `pool`.
    fn admit_v3(by: &U, who: &M, pool: &str, clock: u64) -> Record {
        Record::Admission(Admission::sign_by_user(
            &by.key,
            by.name,
            who.name,
            who.id.public(),
            "h:1",
            pool,
            "",
            clock,
        ))
    }

    fn remove_v3(by: &U, who: &M, clock: u64) -> Record {
        Record::Removal(Removal::sign_by_user(
            &by.key,
            by.name,
            who.name,
            who.id.public(),
            clock,
        ))
    }

    fn add_user(by: &U, who: &U, clock: u64) -> Record {
        Record::User(UserAdmission::sign(
            &by.key,
            by.name,
            who.name,
            who.key.public(),
            clock,
        ))
    }

    fn remove_user(t: &Trust, by: &U, who: &U, clock: u64) -> Record {
        let kept = t.signed_by(&who.key.public());
        Record::UserRemoval(UserRemoval::sign(
            &by.key,
            by.name,
            who.name,
            who.key.public(),
            kept,
            clock,
        ))
    }

    fn user_names(v: &View) -> Vec<String> {
        let mut n: Vec<String> = v.users.values().map(|u| u.name.clone()).collect();
        n.sort();
        n
    }

    /// A cluster a, b, c (a founded it, b and c in that order), and its users: `u` first, its
    /// genesis co-signed by `cosigner`, each member signed in by `u`.
    fn with_users(a: &M, b: &M, c: &M, u: &U, cosigner: &M) -> Trust {
        let mut t = Trust::found(&a.id, "a", "h:1");
        t.merge(vec![admit(a, b, 1), admit(a, c, 2)]);
        let clock = t.next_clock();
        let mut batch = vec![genesis(u, cosigner, clock)];
        for m in [a, b, c] {
            batch.push(admit_v3(u, m, DEFAULT_POOL, clock + 1));
        }
        let changes = t.merge(batch);
        assert!(changes.is_empty(), "{changes:?}");
        assert!(t.users_exist());
        t
    }

    // Owner's design (two tiers, 2026-11-04): records signed before users keep their bytes, so
    // their ids (which removals list in `kept`) and their JSON stay as they were.
    #[test]
    fn records_members_signed_keep_their_bytes_and_ids() {
        let (a, b) = (machine("a"), machine("b"));
        let Record::Admission(adm) = admit(&a, &b, 1) else {
            unreachable!()
        };
        let v2 = serde_json::to_vec(&(
            "potluck admission v2",
            &adm.node,
            &adm.key,
            &adm.address,
            &adm.by,
            &adm.by_key,
            adm.at,
            adm.clock,
            &adm.nonce,
        ))
        .unwrap();
        assert_eq!(adm.id(), RecordId::of(&v2));
        assert!(adm.by_key.verify(&v2, &adm.sig));
        let json = serde_json::to_value(Record::Admission(adm.clone())).unwrap();
        let fields: BTreeSet<&str> = json
            .as_object()
            .unwrap()
            .keys()
            .map(String::as_str)
            .collect();
        assert_eq!(
            fields,
            BTreeSet::from([
                "kind", "node", "key", "address", "by", "by_key", "at", "clock", "nonce", "sig"
            ])
        );
        let t = Trust::found(&a.id, "a", "h:1");
        let Record::Removal(rm) = remove(&t, &a, &b, 2) else {
            unreachable!()
        };
        let v2 = serde_json::to_vec(&(
            "potluck removal v2",
            &rm.node,
            &rm.key,
            &rm.by,
            &rm.by_key,
            rm.at,
            rm.clock,
            &rm.kept,
        ))
        .unwrap();
        assert_eq!(rm.id(), RecordId::of(&v2));
        let json = serde_json::to_value(Record::Removal(rm.clone())).unwrap();
        assert!(
            json.get("by_user").is_none() && json.get("pool").is_none(),
            "{json}"
        );
        // A record saved before users reads back the same.
        let back: Record = serde_json::from_value(json).unwrap();
        assert_eq!(back.id(), rm.id());
        assert!(back.verify());
        // A user's admission of a machine signs other bytes: no v2 record can pass for one.
        let u = user("u");
        let Record::Admission(v3) = admit_v3(&u, &b, "cloud", 1) else {
            unreachable!()
        };
        let mut as_v2 = v3.clone();
        as_v2.pool = None;
        assert!(v3.verify() && !as_v2.verify());
    }

    // Owner's design (two tiers): the first user, signed by its own key and co-signed by a
    // member's machine key, and its admission of every member, taken together: the members stay
    // members. From then on what machines sign counts for nothing; before, what users sign does.
    #[test]
    fn once_a_user_counts_machines_sign_nothing_that_counts() {
        let (a, b, c, d) = (machine("a"), machine("b"), machine("c"), machine("d"));
        let u = user("u");
        // Before users: a user's admission of d counts for nothing.
        let mut t = Trust::found(&a.id, "a", "h:1");
        t.merge(vec![admit(&a, &b, 1), admit(&a, &c, 2)]);
        t.merge(vec![add_user(&u, &u, 3), admit_v3(&u, &d, DEFAULT_POOL, 4)]);
        assert_eq!(names(&t.view), ["a", "b", "c"]);
        assert!(!t.users_exist());
        // A first user's admission without a machine's signature counts for nothing either.
        let mut g = UserAdmission::genesis(&u.key, "u", "b", b.id.public(), 5);
        t.merge(vec![Record::User(g.clone())]);
        assert!(!t.users_exist());
        // Nor one a machine co-signed whose name or key isn't its own.
        g.machine.as_mut().unwrap().sig = a.id.sign(&g.signed_bytes());
        assert!(!g.verify());

        let mut t = with_users(&a, &b, &c, &u, &b);
        assert_eq!(names(&t.view), ["a", "b", "c"]);
        assert_eq!(user_names(&t.view), ["u"]);
        assert!(t.recognized_user(&u.key.public()));
        assert!(!t.recognized_user(&a.id.public()));
        assert!(t.view.members.values().all(|m| m.by_user()));
        // A member's admission of d, and its removal of c: nothing.
        let clock = t.next_clock();
        t.merge(vec![admit(&a, &d, clock), remove(&t, &a, &c, clock)]);
        assert_eq!(names(&t.view), ["a", "b", "c"]);
        assert!(t.view.removed.is_empty());
        // A user's: they count.
        let clock = t.next_clock();
        t.merge(vec![admit_v3(&u, &d, DEFAULT_POOL, clock)]);
        assert_eq!(names(&t.view), ["a", "b", "c", "d"]);
        let clock = t.next_clock();
        t.merge(vec![remove_v3(&u, &c, clock)]);
        assert_eq!(names(&t.view), ["a", "b", "d"]);
        assert!(t.removed_for_good(&c.id.public()));
        // c can't come back, even by a user's admission.
        t.merge(vec![admit_v3(&u, &c, DEFAULT_POOL, clock + 1)]);
        assert_eq!(names(&t.view), ["a", "b", "d"]);
        assert!(t.why_not_member(&c.id.public()).contains("removed by u"));
        same_in_any_order(&t.cluster, t.records());
        // The proof a pairing reply carries implies the same members and users.
        let proof = View::compute(&t.cluster, &t.members_proof());
        assert_eq!(proof.members, t.view.members);
        assert_eq!(proof.users, t.view.users);
        assert_eq!(proof.genesis, t.view.genesis);
        let counting = View::compute(&t.cluster, &t.counting());
        assert_eq!(counting.standing(), t.view.standing());
    }

    // Owner's design (two tiers): of first users' admissions that compete, the one co-signed by
    // the member admitted earliest counts, whatever the clocks; a member admitted later can't
    // keep it from counting by removing the one that co-signed it (a removal that one could
    // undo), and a machine only users admitted co-signs nothing that counts.
    #[test]
    fn the_first_user_co_signed_by_the_member_admitted_first_counts() {
        let (a, b, c, d) = (machine("a"), machine("b"), machine("c"), machine("d"));
        let (u, v) = (user("u"), user("v"));
        let t = with_users(&a, &b, &c, &u, &b);
        // c, admitted after b, co-signs v's first-user admission, with an earlier clock.
        let mut records = t.records().to_vec();
        records.push(genesis(&v, &c, 3));
        records.push(admit_v3(&v, &d, DEFAULT_POOL, 4));
        let view = same_in_any_order(&t.cluster, &records);
        assert_eq!(user_names(&view), ["u"]);
        assert_eq!(names(&view), ["a", "b", "c"]);
        // c removes b (as before users), not keeping b's records: b ranks before c, so for the
        // first user that is a removal b could undo, and u still counts.
        let mut t2 = t.clone();
        let removal = remove_keeping(&c, &b, Vec::new(), t2.next_clock());
        records.push(removal.clone());
        t2.merge(vec![removal]);
        assert_eq!(user_names(&t2.view), ["u"]);
        assert_eq!(names(&t2.view), ["a", "b", "c"]);
        let view = same_in_any_order(&t.cluster, &records);
        assert_eq!(user_names(&view), ["u"]);
        // The founder's co-signature counts before b's.
        let w = user("w");
        records.push(genesis(&w, &a, 50));
        let view = same_in_any_order(&t.cluster, &records);
        assert_eq!(user_names(&view), ["w"]);
        assert!(view.members.is_empty(), "w signed no machine in");
        // A machine users admitted (d) co-signs a first user: not even taken.
        let mut t3 = t.clone();
        let clock = t3.next_clock();
        t3.merge(vec![admit_v3(&u, &d, DEFAULT_POOL, clock)]);
        assert_eq!(names(&t3.view), ["a", "b", "c", "d"]);
        let n = t3.records().len();
        t3.merge(vec![genesis(&v, &d, clock + 1), admit(&d, &c, clock + 2)]);
        assert_eq!(t3.records().len(), n);
        assert_eq!(user_names(&t3.view), ["u"]);
    }

    // Owner's design (two tiers): only a user's signed record adds or removes a user; a removal
    // keeps what the removed user signed that counted where it was signed, and voids the rest.
    #[test]
    fn users_add_and_remove_users() {
        let (a, b, c, d, e) = (
            machine("a"),
            machine("b"),
            machine("c"),
            machine("d"),
            machine("e"),
        );
        let (u, v, x, y) = (user("u"), user("v"), user("x"), user("y"));
        let mut t = with_users(&a, &b, &c, &u, &a);
        // u adds v, v adds x; x admits d.
        let clock = t.next_clock();
        t.merge(vec![
            add_user(&u, &v, clock),
            add_user(&v, &x, clock + 1),
            admit_v3(&x, &d, DEFAULT_POOL, clock + 2),
        ]);
        assert_eq!(user_names(&t.view), ["u", "v", "x"]);
        assert_eq!(names(&t.view), ["a", "b", "c", "d"]);
        // A machine's key adds no user, nor does a key no user admitted.
        let clock = t.next_clock();
        let by_machine = Record::User(UserAdmission::sign(&a.id, "a", "y", y.key.public(), clock));
        t.merge(vec![by_machine, add_user(&y, &y, clock)]);
        assert_eq!(user_names(&t.view), ["u", "v", "x"]);
        // u removes v, keeping what v signed: x and d stay.
        let snapshot = t.clone();
        let clock = t.next_clock();
        let removal = remove_user(&t, &u, &v, clock);
        t.merge(vec![removal]);
        assert_eq!(user_names(&t.view), ["u", "x"]);
        assert_eq!(names(&t.view), ["a", "b", "c", "d"]);
        assert!(!t.recognized_user(&v.key.public()));
        // v, removed, signs a user in and a machine in, dated before its removal: void.
        t.merge(vec![add_user(&v, &y, 3), admit_v3(&v, &e, DEFAULT_POOL, 3)]);
        assert_eq!(user_names(&t.view), ["u", "x"]);
        assert_eq!(names(&t.view), ["a", "b", "c", "d"]);
        // A removal of v that keeps none of v's records voids v's admission of x, and x's of d.
        let mut t2 = snapshot.clone();
        let clock = t2.next_clock();
        t2.merge(vec![Record::UserRemoval(UserRemoval::sign(
            &u.key,
            "u",
            "v",
            v.key.public(),
            Vec::new(),
            clock,
        ))]);
        assert_eq!(user_names(&t2.view), ["u"]);
        assert_eq!(names(&t2.view), ["a", "b", "c"]);
        assert!(t2.why_not_member(&d.id.public()).contains("x"));
        // u and v remove each other, neither knowing: u, the first user, wins.
        let mut t3 = snapshot.clone();
        let clock = t3.next_clock();
        let u_removes_v = remove_user(&t3, &u, &v, clock);
        let v_removes_u = remove_user(&t3, &v, &u, clock);
        t3.merge(vec![v_removes_u, u_removes_v]);
        assert_eq!(user_names(&t3.view), ["u", "x"]);
        same_in_any_order(&t3.cluster, t3.records());
        // A user name is one user's.
        let mut t4 = snapshot;
        let clock = t4.next_clock();
        let twin = U {
            name: "v",
            ..user("v")
        };
        t4.merge(vec![add_user(&u, &twin, clock)]);
        assert_eq!(user_names(&t4.view), ["u", "v", "x"]);
        assert_eq!(
            t4.view.users.values().find(|w| w.name == "v").unwrap().key,
            v.key.public()
        );
    }

    // Owner's design (two tiers): a user's admission gives a machine a pool (`default` unless
    // given); a machine a member admitted is in `default`.
    #[test]
    fn a_machine_is_in_the_pool_its_admission_names() {
        let (a, b, c, d) = (machine("a"), machine("b"), machine("c"), machine("d"));
        let u = user("u");
        let legacy = {
            let mut t = Trust::found(&a.id, "a", "h:1");
            t.merge(vec![admit(&a, &b, 1)]);
            t
        };
        assert_eq!(legacy.pool_of(&b.id.public()), Some(DEFAULT_POOL));
        let mut t = with_users(&a, &b, &c, &u, &a);
        let clock = t.next_clock();
        t.merge(vec![admit_v3(&u, &d, "cloud", clock)]);
        assert_eq!(t.pool_of(&d.id.public()), Some("cloud"));
        assert_eq!(t.pool_of(&b.id.public()), Some(DEFAULT_POOL));
        assert_eq!(t.pool_of(&machine("z").id.public()), None);
        let proof = View::compute(&t.cluster, &t.members_proof());
        assert_eq!(proof.members[&d.id.public()].pool_name(), "cloud");
        // The record and its pool round-trip.
        let rec = t
            .records()
            .iter()
            .find(|r| matches!(r, Record::Admission(a) if a.key == d.id.public()))
            .unwrap()
            .clone();
        let json = serde_json::to_string(&rec).unwrap();
        assert!(json.contains("\"pool\":\"cloud\""), "{json}");
        let back: Record = serde_json::from_str(&json).unwrap();
        assert!(back.verify() && back.id() == rec.id());
        assert!(valid_pool("cloud") && !valid_pool("-x") && !valid_pool("a b"));
    }

    // A store made from what a pairing reply carries, in a cluster with users, takes the rest of
    // the records as members exchange them.
    #[test]
    fn a_store_joining_a_cluster_with_users_agrees_with_it() {
        let (a, b, c, d) = (machine("a"), machine("b"), machine("c"), machine("d"));
        let (u, v) = (user("u"), user("v"));
        let mut t = with_users(&a, &b, &c, &u, &b);
        let clock = t.next_clock();
        t.merge(vec![
            add_user(&u, &v, clock),
            admit_v3(&v, &d, "cloud", clock + 1),
        ]);
        let mut joined = Trust::found(&d.id, "d", "h:1");
        joined.adopt(PairReply {
            v: 1,
            cluster: t.cluster,
            key: d.id.public(),
            nonce: String::new(),
            records: t.members_proof(),
            addresses: Vec::new(),
        });
        assert_eq!(joined.view.members, t.view.members);
        assert_eq!(joined.view.users, t.view.users);
        joined.merge(t.records().to_vec());
        assert_eq!(joined.view.standing(), t.view.standing());
    }

    // Two tiers, random records of both: v2 admissions and removals by machines, first users
    // co-signed by machines, users' admissions and removals of users and of machines. The view
    // doesn't depend on the order records come in; two stores that take them in random batches
    // and then exchange agree; and the members' proof implies the same members and users.
    #[test]
    fn stores_with_users_agree_once_they_exchange_records() {
        let keys: Vec<M> = ["a", "b", "c", "d", "e", "f"]
            .into_iter()
            .map(machine)
            .collect();
        let users: Vec<U> = ["u", "v", "w"].into_iter().map(user).collect();
        let mut seed: u64 = 0x9e37_79b9_7f4a_7c15;
        let mut next = |n: u64| {
            seed ^= seed << 13;
            seed ^= seed >> 7;
            seed ^= seed << 17;
            seed % n
        };
        let names = ["a", "b", "c", "x"];
        let mut with_users = 0;
        for case in 0..200 {
            let limit = [2, 3, MAX_MEMBERS][next(3) as usize];
            let mut records: Vec<Record> = Vec::new();
            for _ in 0..(4 + next(16)) {
                let clock = 1 + next(10);
                let m = &keys[next(6) as usize];
                let subject = &keys[1 + next(5) as usize];
                let u = &users[next(3) as usize];
                let other = &users[next(3) as usize];
                let r = match next(10) {
                    0..=2 => admit_as(m, names[next(4) as usize], subject.id.public(), clock),
                    3 => {
                        let kept = records
                            .iter()
                            .filter(|r| r.signer() == subject.id.public() && next(3) > 0)
                            .map(Record::id)
                            .collect();
                        remove_keeping(m, subject, kept, clock)
                    }
                    4 => genesis(u, m, clock),
                    5 => add_user(u, other, clock),
                    6 => {
                        let kept = records
                            .iter()
                            .filter(|r| r.signer() == other.key.public() && next(3) > 0)
                            .map(Record::id)
                            .collect();
                        Record::UserRemoval(UserRemoval::sign(
                            &u.key,
                            u.name,
                            other.name,
                            other.key.public(),
                            kept,
                            clock,
                        ))
                    }
                    7 | 8 => Record::Admission(Admission::sign_by_user(
                        &u.key,
                        u.name,
                        names[next(4) as usize],
                        subject.id.public(),
                        "h:1",
                        ["default", "cloud"][next(2) as usize],
                        "",
                        clock,
                    )),
                    _ => remove_v3(u, subject, clock),
                };
                records.push(r);
            }
            let cluster = keys[0].id.public();
            let view = same_in_any_order(&cluster, &{
                let mut all = store(&keys[0], &keys[0], limit).records().to_vec();
                all.extend(records.iter().cloned());
                all
            });
            with_users += usize::from(view.users_exist());
            let mut x = store(&keys[0], &keys[next(6) as usize], limit);
            let mut y = store(&keys[0], &keys[next(6) as usize], limit);
            y.records = x.records.clone();
            y.recompute();
            let (mut to_x, mut to_y) = (Vec::new(), Vec::new());
            for r in &records {
                match next(3) {
                    0 => to_x.push(r.clone()),
                    1 => to_y.push(r.clone()),
                    _ => {
                        to_x.push(r.clone());
                        to_y.push(r.clone());
                    }
                }
            }
            for (t, mut rs) in [(&mut x, to_x), (&mut y, to_y)] {
                while !rs.is_empty() {
                    let n = 1 + next(rs.len() as u64) as usize;
                    t.merge(rs.drain(..n).collect());
                }
            }
            for _ in 0..10 {
                let to_y = y.merge(x.records().to_vec());
                let to_x = x.merge(y.records().to_vec());
                if !to_y.stored && !to_x.stored {
                    break;
                }
            }
            assert_eq!(x.view.standing(), y.view.standing(), "case {case}");
            assert!(x.view.members.len() <= limit, "case {case}");
            let proof = View::compute(&cluster, &x.members_proof());
            assert_eq!(
                (&proof.members, &proof.users, &proof.genesis),
                (&x.view.members, &x.view.users, &x.view.genesis),
                "case {case}"
            );
            let counting = View::compute(&cluster, &x.counting());
            assert_eq!(
                (&counting.members, &counting.users, &counting.genesis),
                (&x.view.members, &x.view.users, &x.view.genesis),
                "case {case}"
            );
        }
        assert!(with_users > 40, "{with_users} of 200 cases had users");
    }
}
