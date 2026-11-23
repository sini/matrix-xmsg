# Removes each argument that is a Unix socket nothing accepts a connection on. A live
# listener, a missing path and any non-socket file are left alone.
{ pkgs }:
pkgs.writeShellScript "reap-dead-sockets" ''
  for sock in "$@"; do
    if [ -S "$sock" ] && ! ${pkgs.socat}/bin/socat -u OPEN:/dev/null "UNIX-CONNECT:$sock" 2>/dev/null; then
      ${pkgs.coreutils}/bin/rm -f -- "$sock"
    fi
  done
''
