#!/usr/bin/env bash
# The host-level half of the whole-replica-set crash benchmark (#599): everything that needs root
# on the Docker host. crash.py runs it with `sudo -n` (or as root) and parses what it prints.
#
# WHAT THIS SIMULATES, precisely. The data directories of all three nodes of one system live on ONE
# ext4 filesystem on ONE device-mapper `flakey` device, which sits on a loop device over a sparse file.
# After the three processes have been SIGKILLed, `drop` reloads the flakey table with `drop_writes`
# WITHOUT freezing or syncing the filesystem (`--nolockfs`), so every write the page cache had not yet
# sent to the device is discarded, and `restore` unmounts, reloads a table that writes again, and
# mounts. What the filesystem holds afterwards is what had reached the device when the table was
# reloaded: data an fsync returned for, and whatever the kernel's own writeback had already flushed.
# That is the loss of the host's page cache for all three replicas at once (a kernel panic, or power
# loss with a disk that honours flushes).
#
# WHAT IT DOES NOT SIMULATE: a drive whose volatile write cache lies about flushes; a torn write
# inside one block; power loss of a host with a hardware write-back cache; independent crashes of the
# replicas (they share one failure domain on purpose); the instant between the kill and the drop (a few
# milliseconds, recorded as kill_to_drop_ms), in which the kernel may flush a little more, which can
# only make a system look better, never worse.
#
# Two canary files make every crash self-checking: one written with fsync completed (must be whole
# after the crash), one written with plain writes and no sync (must be absent or short when the cache
# is dropped).
#
# Usage: powercut.sh <command> <root> [args]     (root is the data root every node's directory is under)
#   setup <root> <size-mib>    create the image, the flakey device, an ext4 filesystem, mount it at <root>
#   teardown <root>            unmount and remove all of it
#   status <root>              print "flakey" if <root> is the mounted flakey device, "plain" otherwise
#   describe <root>            key=value lines for environment.json
#   sync <root>                flush that filesystem (before a crash, so only the load's data is at risk)
#   canary-synced <root>       write the canary that is fsynced
#   canary-unsynced <root>     write the canary that is not
#   canary-check <root>        print "synced=<bytes> unsynced=<bytes>"
#   drop <root>                drop every write from now on (after the SIGKILL)
#   restore <root>             unmount, write again, mount
set -euo pipefail

CANARY_BYTES=1048576

die() {
    echo "powercut: $*" >&2
    exit 1
}

command_name="${1:-}"
root="${2:-}"
[[ -n "$command_name" && -n "$root" ]] || die "usage: powercut.sh <command> <root> [args]"
root="${root%/}"
image="${root}.img"
state="${root}.flakey"
name="kc599-$(basename "$root")"
synced_file="${root}/.kc599-canary-synced"
unsynced_file="${root}/.kc599-canary-unsynced"

load_state() {
    [[ -f "$state" ]] || die "no flakey state at $state: run 'setup' first, or use a process-kill crash mode"
    # shellcheck source=/dev/null
    source "$state"
}

table_allow() { echo "0 ${sectors} flakey ${loopdev} 0 180 0"; }
table_drop() { echo "0 ${sectors} flakey ${loopdev} 0 0 180 1 drop_writes"; }

# Reload the flakey table without letting `dmsetup suspend` freeze and sync the filesystem: that sync
# would save exactly what a power loss loses.
reload_table() {
    local table="$1"
    dmsetup suspend --nolockfs --noflush "$name" 2>/dev/null || dmsetup suspend --nolockfs "$name"
    dmsetup load "$name" --table "$table"
    dmsetup resume "$name"
}

case "$command_name" in
setup)
    size_mib="${3:?setup needs a size in MiB}"
    [[ ! -e "$state" ]] || die "$state exists: a flakey device is already set up for $root"
    truncate -s "${size_mib}M" "$image"
    loopdev="$(losetup --find --show "$image")"
    sectors="$(blockdev --getsz "$loopdev")"
    dmsetup create "$name" --table "0 ${sectors} flakey ${loopdev} 0 180 0"
    mkfs.ext4 -q -F "/dev/mapper/${name}"
    mkdir -p "$root"
    mount "/dev/mapper/${name}" "$root"
    chmod 0777 "$root"
    printf 'loopdev=%q\nsectors=%q\n' "$loopdev" "$sectors" >"$state"
    echo "flakey device /dev/mapper/${name} on ${loopdev} (${sectors} sectors), ext4, mounted at ${root}"
    ;;
teardown)
    load_state
    umount "$root" 2>/dev/null || true
    dmsetup remove "$name" 2>/dev/null || true
    losetup -d "$loopdev" 2>/dev/null || true
    rm -f "$image" "$state"
    ;;
status)
    if [[ -f "$state" ]] && findmnt -n -o SOURCE "$root" 2>/dev/null | grep -q "/dev/mapper/${name}"; then
        echo flakey
    else
        echo plain
    fi
    ;;
describe)
    echo "kernel=$(uname -r)"
    echo "fs_type=$(findmnt -n -o FSTYPE --target "$root" 2>/dev/null || echo unknown)"
    echo "mount_options=$(findmnt -n -o OPTIONS --target "$root" 2>/dev/null || echo unknown)"
    echo "mount_source=$(findmnt -n -o SOURCE --target "$root" 2>/dev/null || echo unknown)"
    for key in dirty_expire_centisecs dirty_writeback_centisecs dirty_ratio dirty_background_ratio; do
        echo "vm.${key}=$(cat "/proc/sys/vm/${key}" 2>/dev/null || echo unknown)"
    done
    ;;
sync)
    sync -f "$root"
    ;;
canary-synced)
    dd if=/dev/zero of="$synced_file" bs="$CANARY_BYTES" count=1 conv=fsync status=none
    sync "$synced_file"
    sync "$root"
    ;;
canary-unsynced)
    dd if=/dev/zero of="$unsynced_file" bs="$CANARY_BYTES" count=1 status=none
    ;;
canary-check)
    synced=0
    unsynced=0
    [[ -f "$synced_file" ]] && synced="$(wc -c <"$synced_file" | tr -d ' ')"
    [[ -f "$unsynced_file" ]] && unsynced="$(wc -c <"$unsynced_file" | tr -d ' ')"
    echo "synced=${synced} unsynced=${unsynced}"
    rm -f "$synced_file" "$unsynced_file"
    ;;
drop)
    load_state
    reload_table "$(table_drop)"
    ;;
restore)
    load_state
    umount "$root" || die "cannot unmount $root: a process still holds it open; remove the containers first"
    reload_table "$(table_allow)"
    mount "/dev/mapper/${name}" "$root"
    ;;
*)
    die "unknown command ${command_name}"
    ;;
esac
