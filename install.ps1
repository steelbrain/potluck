# Installs potluck on Windows from a GitHub release: picks the binary for this machine's
# architecture, checks its SHA-256 against the release's SHA256SUMS, and installs it as
# $env:POTLUCK_INSTALL_DIR\potluck.exe (default %USERPROFILE%\.local\bin\potluck.exe). It never
# asks for elevation and never prompts. Runs in Windows PowerShell 5.1 and PowerShell 7.
#
#   irm https://github.com/steelbrain/potluck/releases/latest/download/install.ps1 | iex
#       installs the latest release
#   & ([scriptblock]::Create((irm https://github.com/steelbrain/potluck/releases/latest/download/install.ps1))) 0.2.0
#       installs release v0.2.0 (the version may also be given as v0.2.0)
#   powershell -ExecutionPolicy Bypass -File install.ps1 [VERSION]
#       the same, from a downloaded copy
#
# Environment:
#   POTLUCK_VERSION         the version to install when none is given as an argument
#   POTLUCK_INSTALL_DIR     the directory to install into (default: %USERPROFILE%\.local\bin)
#   POTLUCK_RELEASES_URL    a directory URL holding one release's binaries and SHA256SUMS, used
#                           instead of GitHub (plain http is allowed); a requested version is then
#                           checked against the version the downloaded binary reports
#   POTLUCK_GITHUB_URL      replaces https://github.com (for tests)
#   POTLUCK_NO_RESTART=1    leave a running potluck daemon alone (by default it restarts in place on
#                           the new binary, keeping running jobs, whether it runs as the scheduled
#                           task or not; the task is restarted when no daemon answers)
#   POTLUCK_NO_MODIFY_PATH=1
#                           leave the user's PATH alone (by default the install directory is added
#                           to it when it isn't there)
#   POTLUCK_SERVICE_NAME    the scheduled task's name, as for `potluck service install` (for tests)

& {
    $ErrorActionPreference = 'Stop'
    $ProgressPreference = 'SilentlyContinue'
    # Run as a file (-File), a failure exits 1. Run with `irm | iex` it only says why: exiting
    # there would close the user's PowerShell.
    $asFile = [bool]$PSCommandPath

    # The GitHub repository the releases come from; keep in step with GITHUB_REPO in src/update.rs.
    $repo = 'steelbrain/potluck'
    $usage = 'usage: install.ps1 [VERSION]   (for example: install.ps1 0.2.0)'

    function Fail([string]$message) {
        throw "install.ps1: $message"
    }

    function Say([string]$message) {
        [Console]::Out.WriteLine($message)
    }

    # One argument for a Windows command line, quoted as programs split it.
    function Format-Argument([string]$arg) {
        if ($arg -and $arg -notmatch '[\s"]') {
            return $arg
        }
        return '"' + (($arg -replace '(\\*)"', '$1$1\"') -replace '(\\+)$', '$1$1') + '"'
    }

    # How to run the program at $path from PowerShell.
    function Format-Command([string]$path) {
        if ($path -match '\s') {
            return "& `"$path`""
        }
        return $path
    }

    # Runs $exe with $arguments, without a window, and returns its exit code and output.
    function Invoke-Native([string]$exe, [string[]]$arguments) {
        $psi = New-Object System.Diagnostics.ProcessStartInfo
        $psi.FileName = $exe
        $psi.Arguments = (@($arguments | ForEach-Object { Format-Argument $_ })) -join ' '
        $psi.UseShellExecute = $false
        $psi.RedirectStandardOutput = $true
        $psi.RedirectStandardError = $true
        $psi.CreateNoWindow = $true
        try {
            $p = [System.Diagnostics.Process]::Start($psi)
        } catch {
            return @{ Started = $false; Code = -1; Out = ''; All = $_.Exception.Message }
        }
        $out = $p.StandardOutput.ReadToEndAsync()
        $err = $p.StandardError.ReadToEndAsync()
        $p.WaitForExit()
        return @{
            Started = $true
            Code = $p.ExitCode
            Out = $out.Result
            All = ($out.Result + $err.Result).Trim()
        }
    }

    # Downloads $url to $file, trying again twice after a failure that isn't an HTTP error.
    function Get-Url([string]$url, [string]$file) {
        $why = ''
        for ($try = 1; $try -le 3; $try++) {
            try {
                $client = New-Object System.Net.WebClient
                $client.Headers['User-Agent'] = 'potluck-install'
                $client.DownloadFile($url, $file)
                return
            } catch {
                $e = $_.Exception
                while ($e.InnerException -and -not ($e -is [System.Net.WebException])) {
                    $e = $e.InnerException
                }
                $why = $e.Message
                if ($e -is [System.Net.WebException] -and $e.Response) {
                    break
                }
                Start-Sleep -Seconds 1
            }
        }
        Fail "couldn't download ${url}: $why"
    }

    # Where $url redirects to, without following it ('' if it doesn't).
    function Get-Redirect([string]$url) {
        $req = [System.Net.HttpWebRequest]::Create($url)
        $req.AllowAutoRedirect = $false
        $req.UserAgent = 'potluck-install'
        try {
            $resp = $req.GetResponse()
        } catch [System.Net.WebException] {
            $resp = $_.Exception.Response
            if (-not $resp) {
                return ''
            }
        }
        try {
            return [string]$resp.Headers['Location']
        } finally {
            $resp.Close()
        }
    }

    # The message of an error from a .NET method, without PowerShell's wrapping.
    function Get-Why($err) {
        $e = $err.Exception
        if ($e.InnerException) {
            $e = $e.InnerException
        }
        return $e.Message
    }

    function Test-SameFile([string]$a, [string]$b) {
        try {
            return [IO.Path]::GetFullPath($a) -eq [IO.Path]::GetFullPath($b)
        } catch {
            return $false
        }
    }

    function Get-Task([string]$name) {
        try {
            return Get-ScheduledTask -TaskName $name -TaskPath '\' -ErrorAction Stop
        } catch {
            return $null
        }
    }

    # Ends the task's running instance, waits for it to be gone, and starts it again.
    function Restart-Task([string]$name) {
        try {
            Stop-ScheduledTask -TaskName $name -TaskPath '\' -ErrorAction Stop
            $giveUp = (Get-Date).AddSeconds(30)
            while ([string](Get-Task $name).State -eq 'Running') {
                if ((Get-Date) -gt $giveUp) {
                    return $false
                }
                Start-Sleep -Milliseconds 200
            }
            Start-ScheduledTask -TaskName $name -TaskPath '\' -ErrorAction Stop
            return $true
        } catch {
            return $false
        }
    }

    function Install-Potluck([object[]]$given) {
        if ($given.Count -gt 1) {
            Fail $usage
        }
        $asked = if ($given.Count -eq 1) { [string]$given[0] } else { [string]$env:POTLUCK_VERSION }
        $version = $asked.Trim()
        if ($version.StartsWith('v')) {
            $version = $version.Substring(1)
        }
        if ($version -and $version -notmatch '^[0-9]') {
            Fail $usage
        }
        if ($version -match '[^0-9A-Za-z.+-]') {
            Fail "not a version: $asked"
        }

        # The machine's own architecture: an x64 PowerShell on Windows on Arm runs emulated and
        # reports AMD64 in PROCESSOR_ARCHITECTURE, but the registry's value is the machine's.
        $arches = @()
        $arches += $env:PROCESSOR_ARCHITEW6432
        $key = 'HKLM:\SYSTEM\CurrentControlSet\Control\Session Manager\Environment'
        $arches += (Get-ItemProperty -Path $key -Name PROCESSOR_ARCHITECTURE -ErrorAction SilentlyContinue).PROCESSOR_ARCHITECTURE
        try {
            $arches += [string][System.Runtime.InteropServices.RuntimeInformation]::OSArchitecture
        } catch {
        }
        $arches += $env:PROCESSOR_ARCHITECTURE
        $arches = @($arches | Where-Object { $_ })
        if ($arches -contains 'ARM64') {
            $target = 'aarch64-pc-windows-msvc'
        } elseif ($arches -contains 'AMD64' -or $arches -contains 'X64') {
            $target = 'x86_64-pc-windows-msvc'
        } else {
            Fail "potluck publishes Windows binaries for x86_64 and arm64, not for $($arches -join '/'); build it from source with cargo instead"
        }
        $asset = "potluck-$target.exe"

        if ($env:POTLUCK_INSTALL_DIR) {
            $dir = $env:POTLUCK_INSTALL_DIR.TrimEnd('\', '/')
        } elseif ($env:USERPROFILE) {
            $dir = Join-Path $env:USERPROFILE '.local\bin'
        } else {
            Fail 'USERPROFILE is not set; set POTLUCK_INSTALL_DIR to the directory to install into'
        }

        # GitHub takes TLS 1.2 or newer, which older .NET defaults leave out.
        try {
            [Net.ServicePointManager]::SecurityProtocol = [Net.ServicePointManager]::SecurityProtocol -bor [Net.SecurityProtocolType]::Tls12
        } catch {
        }

        # The version the binary must report: the requested one, or the latest release's.
        $expect = $version
        $tagged = $false
        if ($env:POTLUCK_RELEASES_URL) {
            $base = $env:POTLUCK_RELEASES_URL.TrimEnd('/')
        } else {
            $github = if ($env:POTLUCK_GITHUB_URL) { $env:POTLUCK_GITHUB_URL } else { 'https://github.com' }
            $root = "$($github.TrimEnd('/'))/$repo/releases"
            if ($version) {
                $tag = "v$version"
            } else {
                # Resolve "latest" to its tag first, so SHA256SUMS and the binary come from one
                # release even if another is published meanwhile.
                $location = ''
                try {
                    $location = Get-Redirect "$root/latest"
                } catch {
                }
                if ($location -notmatch '/tag/(v[0-9][^?#]*)') {
                    Fail "couldn't find the latest release: $root/latest leads to '$location', not a release tag"
                }
                $tag = [Uri]::UnescapeDataString($Matches[1])
                if ($tag.Substring(1) -match '[^0-9A-Za-z.+-]') {
                    Fail "couldn't find the latest release: unexpected tag '$tag' at $location"
                }
                $expect = $tag.Substring(1)
            }
            $base = "$root/download/$tag"
            $tagged = $true
        }

        $tmp = Join-Path ([IO.Path]::GetTempPath()) ('potluck-install-' + [Guid]::NewGuid().ToString('N'))
        New-Item -ItemType Directory -Path $tmp | Out-Null
        $staged = $null
        try {
            $sums = Join-Path $tmp 'SHA256SUMS'
            Get-Url "$base/SHA256SUMS" $sums
            $expected = $null
            foreach ($line in [IO.File]::ReadAllLines($sums)) {
                $parts = $line.Trim() -split '\s+', 2
                if ($parts.Count -eq 2 -and ($parts[1] -ceq $asset -or $parts[1] -ceq "*$asset")) {
                    $expected = $parts[0].ToLowerInvariant()
                    break
                }
            }
            if (-not $expected) {
                Fail "$base/SHA256SUMS has no line for $asset"
            }

            $download = Join-Path $tmp $asset
            Get-Url "$base/$asset" $download
            $actual = (Get-FileHash -Algorithm SHA256 -LiteralPath $download).Hash.ToLowerInvariant()
            if ($actual -ne $expected) {
                $race = ''
                if (-not $tagged) {
                    $race = ' (if a release was published there between the two downloads, run it again)'
                }
                Fail "$base/$asset has SHA-256 $actual, but SHA256SUMS lists $expected; refusing it, nothing was installed$race"
            }

            # Write the new binary beside the old one, check that it runs there, and move it into
            # place. Windows won't write over or delete a running potluck.exe, but renames it:
            # the old one is renamed aside, as `potluck self-update` does.
            try {
                New-Item -ItemType Directory -Force -Path $dir | Out-Null
            } catch {
                Fail "can't create $dir"
            }
            $named = Join-Path $dir 'potluck.exe'
            if (Test-Path -LiteralPath $named -PathType Container) {
                Fail "$named is a directory"
            }
            # A symlink stays one: the file it leads to is replaced, as `potluck self-update` does.
            $dest = $named
            $links = 0
            while ($true) {
                $item = Get-Item -LiteralPath $dest -Force -ErrorAction SilentlyContinue
                if (-not $item -or [string]$item.LinkType -ne 'SymbolicLink') {
                    break
                }
                $links++
                if ($links -gt 40) {
                    Fail "${named}: too many levels of symbolic links"
                }
                $link = [string]@($item.Target)[0]
                if (-not [IO.Path]::IsPathRooted($link)) {
                    $link = Join-Path (Split-Path -Parent $dest) $link
                }
                $dest = [IO.Path]::GetFullPath($link)
            }
            $destDir = Split-Path -Parent $dest
            if (-not (Test-Path -LiteralPath $destDir -PathType Container)) {
                Fail "$named links to $dest, whose directory doesn't exist"
            }
            $leaf = Split-Path -Leaf $dest
            $staged = Join-Path $destDir ".$leaf.install-$PID.exe"
            try {
                Copy-Item -LiteralPath $download -Destination $staged -Force
            } catch {
                Fail "can't write to $destDir"
            }
            try {
                Unblock-File -LiteralPath $staged
            } catch {
            }
            $ran = Invoke-Native $staged @('--version')
            if (-not $ran.Started -or $ran.Code -ne 0) {
                Fail "the downloaded $asset doesn't run on this machine; nothing was installed"
            }
            $reported = ([string]$ran.Out -split "`r?`n")[0].Trim()
            if ($reported -notmatch '^potluck ([0-9A-Za-z.+-]+)') {
                Fail "the downloaded $asset printed '$reported' for --version; nothing was installed"
            }
            $newVersion = $Matches[1]
            # Build metadata (+...) doesn't make another version.
            $core = { param($v) ($v -split '\+')[0] }
            if ($version -and (& $core $newVersion) -ne (& $core $version)) {
                Fail "$base has potluck $newVersion, not the requested $version; nothing was installed"
            } elseif ($expect -and (& $core $newVersion) -ne (& $core $expect)) {
                Fail "$base has potluck $newVersion, not $expect as its tag says; nothing was installed"
            }

            # Binaries renamed aside before, that nothing runs any more, go now.
            $prefix = ".$leaf.old-"
            Get-ChildItem -LiteralPath $destDir -Force -ErrorAction SilentlyContinue |
                Where-Object { $_.Name.StartsWith($prefix) } |
                ForEach-Object { Remove-Item -LiteralPath $_.FullName -Force -ErrorAction SilentlyContinue }
            $aside = $null
            if (Test-Path -LiteralPath $dest) {
                $aside = Join-Path $destDir ($prefix + "$PID-" + [DateTime]::UtcNow.Ticks)
                try {
                    [IO.File]::Move($dest, $aside)
                } catch {
                    Fail "can't rename $dest aside to make room for the new binary: $(Get-Why $_)"
                }
            }
            try {
                [IO.File]::Move($staged, $dest)
            } catch {
                $why = "can't move the new binary to ${dest}: $(Get-Why $_)"
                if ($aside) {
                    try {
                        [IO.File]::Move($aside, $dest)
                    } catch {
                        Fail "$why; moving the old binary back failed too: it is $aside; rename it to $dest"
                    }
                    Fail "$why; the old binary was put back"
                }
                Fail $why
            }
            $staged = $null
            if ($aside) {
                # A running binary can't be removed; the next install or update removes it.
                Remove-Item -LiteralPath $aside -Force -ErrorAction SilentlyContinue
            }
        } finally {
            Remove-Item -LiteralPath $tmp -Recurse -Force -ErrorAction SilentlyContinue
            if ($staged) {
                Remove-Item -LiteralPath $staged -Force -ErrorAction SilentlyContinue
            }
        }
        if ($dest -eq $named) {
            Say "Installed potluck $newVersion to $named."
        } else {
            Say "Installed potluck $newVersion to $dest ($named links to it)."
        }

        # The daemon's scheduled task, and the program it runs (`potluck service install` writes
        # the executable it ran from, symlinks resolved).
        $taskName = if ($env:POTLUCK_SERVICE_NAME) { $env:POTLUCK_SERVICE_NAME } else { 'potluck' }
        $service = 'none'
        $program = $null
        $task = Get-Task $taskName
        if ($task) {
            $service = if ([string]$task.State -eq 'Running') { 'running' } else { 'stopped' }
            $exec = @($task.Actions | Where-Object { $_.Execute }) | Select-Object -First 1
            if ($exec) {
                $program = ([string]$exec.Execute).Trim().Trim('"')
            }
        }
        $run = Format-Command $named
        # A task installed from another potluck keeps running that one: restarting it would not
        # run this version.
        if ($service -ne 'none' -and $program -and -not (Test-SameFile $program $dest)) {
            Say "The potluck service runs $program, not $named, so it was not restarted. To make it run ${named}: $run service install"
            $service = 'other'
        }
        if ($service -eq 'running') {
            if ($env:POTLUCK_NO_RESTART -eq '1') {
                Say "The potluck service still runs the old binary; restart it with: $run restart"
            } else {
                # The daemon restarts in place on the new binary (`potluck restart`). Exit 4: no
                # daemon answers here: the task is restarted.
                $r = Invoke-Native $named @('restart')
                $restart = "schtasks /end /tn $taskName; schtasks /run /tn $taskName"
                if ($r.Code -eq 0) {
                    Say $r.All
                } elseif ($r.Code -ne 4) {
                    Fail "installed, but the daemon did not restart in place: $($r.All)"
                } elseif (Restart-Task $taskName) {
                    Say "Restarted the potluck service ($restart)."
                } else {
                    Fail "installed, but restarting the potluck service failed; restart it with: $restart"
                }
            }
        } elseif ($service -eq 'stopped') {
            Say 'The potluck service is installed but not running; it will run the new binary when it starts.'
        }
        # A daemon running here without the task (`potluck daemon` in a terminal) restarts in place
        # on the new binary too. Exit 4: no daemon answers here.
        if (($service -eq 'none' -or $service -eq 'stopped') -and $env:POTLUCK_NO_RESTART -ne '1' -and $env:USERPROFILE) {
            $r = Invoke-Native $named @('restart')
            if ($r.Code -eq 0) {
                Say $r.All
            } elseif ($r.Code -ne 4) {
                Fail "installed, but the potluck daemon running here did not restart in place: $($r.All)"
            }
        }

        # The user's PATH, as the registry holds it (variables such as %USERPROFILE% unexpanded).
        $norm = { param($p) [Environment]::ExpandEnvironmentVariables($p).TrimEnd('\').ToLowerInvariant() }
        $want = & $norm $dir
        $envKey = [Microsoft.Win32.Registry]::CurrentUser.OpenSubKey('Environment', $true)
        $userPath = [string]$envKey.GetValue('Path', '', 'DoNotExpandEnvironmentNames')
        $machinePath = [string][Environment]::GetEnvironmentVariable('Path', 'Machine')
        $onPath = @(($userPath + ';' + $machinePath) -split ';' | Where-Object { $_ } | ForEach-Object { & $norm $_ }) -contains $want
        if (-not $onPath) {
            if ($env:POTLUCK_NO_MODIFY_PATH -eq '1') {
                Say "Note: $dir is not on your PATH; add it to your Path user environment variable."
            } else {
                $kind = if ($userPath) { $envKey.GetValueKind('Path') } else { [Microsoft.Win32.RegistryValueKind]::ExpandString }
                $newPath = if ($userPath) { $userPath.TrimEnd(';') + ';' + $dir } else { $dir }
                $envKey.SetValue('Path', $newPath, $kind)
                # Tells running programs (Explorer, and the terminals it starts) the environment
                # changed; the variable it removes was never set.
                [Environment]::SetEnvironmentVariable('POTLUCK_INSTALL_UNSET', $null, 'User')
                $env:Path = $env:Path.TrimEnd(';') + ';' + $dir
                Say "Added $dir to your user PATH; terminals opened from now on find potluck there."
            }
        }
        $envKey.Close()
        $found = Get-Command potluck -CommandType Application -ErrorAction SilentlyContinue | Select-Object -First 1
        if ($found -and -not (Test-SameFile $found.Path $named)) {
            Say "Note: $($found.Path) comes before $named on your PATH."
        }

        if ($service -eq 'none') {
            Say ''
            Say 'Next steps:'
            $state = if ($env:POTLUCK_HOME) { $env:POTLUCK_HOME } else { [Environment]::GetEnvironmentVariable('POTLUCK_HOME', 'User') }
            $defaultFs = ''
            try {
                $defaultFs = (New-Object System.IO.DriveInfo ([IO.Path]::GetPathRoot($env:USERPROFILE))).DriveFormat
            } catch {
            }
            if (-not $state -and $defaultFs -ne 'ReFS') {
                Say "  [Environment]::SetEnvironmentVariable('POTLUCK_HOME', 'D:\potluck', 'User')"
                Say '                             (potluck keeps its state on a Dev Drive or another ReFS volume: use a directory on yours, then open a new terminal)'
            }
            Say '  potluck service install    (runs the daemon as a scheduled task while you are signed in)'
            Say "  potluck user init          (on the first machine: makes this command line's key the first user, which dispatch, admit and remove need)"
            Say '  potluck pair               (on a machine joining a cluster: prints a line for `potluck admit` on a member)'
        }
    }

    try {
        Install-Potluck -given $args
    } catch {
        $message = [string]$_.Exception.Message
        if (-not $message.StartsWith('install.ps1: ')) {
            $message = "install.ps1: $message"
        }
        [Console]::Error.WriteLine($message)
        if ($asFile) {
            exit 1
        }
    }
} @args
