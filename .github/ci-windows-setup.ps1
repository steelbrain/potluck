# Prepares a GitHub-hosted Windows runner for potluck's tests. ci.yml runs it from the checkout,
# before anything is built. It changes the machine (a virtual disk mounted over ./target, a
# Defender exclusion), so it is for throwaway CI runners only, not for a development machine.
#
# ./target becomes a ReFS volume in a sparse virtual disk. The workspace, tunnel and e2e tests keep
# their temp dirs under the target directory (CARGO_TARGET_TMPDIR is target\tmp), and these must
# be on a filesystem that can clone files; the runner's own disk is NTFS. Windows Server formats
# ReFS; Windows 11 formats it only as a Dev Drive, which needs at least 50 GB.
$ErrorActionPreference = 'Stop'

$target = Join-Path $PWD 'target'
$vhd = Join-Path $env:RUNNER_TEMP 'potluck-target.vhdx'
$script = Join-Path $env:RUNNER_TEMP 'potluck-target.diskpart'
New-Item -ItemType Directory -Force $target | Out-Null

# diskpart, because Hyper-V's New-VHD isn't on the runners. The disk is sparse (expandable): it
# takes only the space written to it.
@(
    "create vdisk file=`"$vhd`" maximum=61440 type=expandable"
    "select vdisk file=`"$vhd`""
    'attach vdisk'
) | Set-Content -Encoding ascii $script
diskpart /s $script
if ($LASTEXITCODE -ne 0) { throw "diskpart failed ($LASTEXITCODE)" }

$disk = Get-DiskImage -ImagePath $vhd | Get-Disk
Initialize-Disk -Number $disk.Number -PartitionStyle GPT
$part = New-Partition -DiskNumber $disk.Number -UseMaximumSize
try {
    $part | Format-Volume -FileSystem ReFS -NewFileSystemLabel potluck -Confirm:$false | Out-Null
} catch {
    Write-Host "ReFS format refused ($_); formatting a Dev Drive"
    $part | Format-Volume -DevDrive -NewFileSystemLabel potluck -Confirm:$false | Out-Null
}
$part | Add-PartitionAccessPath -AccessPath "$target\"

$fs = (Get-Volume -Partition (Get-Partition -DiskNumber $disk.Number -PartitionNumber $part.PartitionNumber)).FileSystem
if ($fs -ne 'ReFS') { throw "$target is on $fs, not ReFS" }

# Real-time scanning of every file the builds write slows them down severalfold.
Add-MpPreference -ExclusionPath $target
Get-Volume -Partition (Get-Partition -DiskNumber $disk.Number -PartitionNumber $part.PartitionNumber) | Format-List FileSystem, Size, SizeRemaining
