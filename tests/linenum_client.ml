open Lwt
open Linenum_common

let lines = 3

let client_loop conn =
  let rec loop i =
    if i > lines
    then return_unit
    else
      lwt () = C.send conn (string_of_int i) in
      lwt r = C.recv conn in
      Printf.printf "response: %s\n%!" r;
      loop (i + 1)
  in
    loop 1

let () = Lwt_main.run begin
  try_lwt
    let open Lwt_unix in
    lwt conn =
      C.connect_unix
        linenum_unix_serverfunc
        PF_INET
        SOCK_STREAM
        0
        (ADDR_INET (Unix.inet_addr_loopback, 34567))
    in
    lwt () = client_loop conn in
    C.close conn;
    return_unit
  with
    e -> Printf.eprintf "%s: %s\n%!" Sys.argv.(0) (Printexc.to_string e);
      return_unit
end
