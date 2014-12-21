open Lwt
open Linenum_common

let _ls_counter = ref 0

let make_linenum_server ?(limit=0) () = C.duplex begin fun conn ->
  incr _ls_counter;
  C.set_conn_name conn ("linenum#" ^ string_of_int !_ls_counter);
  let rec loop n =
    match_lwt C.recv_opt conn with
    | None ->
        C.close conn;
        (*- Printf.eprintf "ls: closed\n%!"; -*)
        return_unit
    | Some line ->
        let out = string_of_int n ^ ": " ^ line in
        lwt () = C.send conn out in
        (*- Printf.eprintf "ls: responded: %s\n%!" out; -*)
        if n = limit
        then begin
          C.shutdown conn Unix.SHUTDOWN_ALL;
          return_unit
        end else
          loop (n + 1)
  in
    try_lwt
      loop 1
    with
    | End_of_file as e ->
        (* Printf.eprintf "linenum_server: exiting with eof\n%!"; *)
        fail e
    | e ->
        Printf.eprintf "linenum_server internal error: %s\n%!"
          (Printexc.to_string e);
        fail e
end
~on_shutdown: begin fun () ->
  Printf.eprintf "linenum_server shut down\n%!";
  return_unit
end

let (linenum_server, server_ctl) = make_linenum_server ()
