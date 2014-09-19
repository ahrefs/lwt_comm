open Lwt
open Linenum_common

let (linenum_server, server_ctl) = C.duplex begin fun conn ->
  let rec loop n =
    match_lwt C.recv_opt conn with
    | None -> C.close conn; return_unit
    | Some line ->
        let out = string_of_int n ^ ": " ^ line in
        lwt () = C.send conn out in
        loop (n + 1)
  in
    loop 1
end
~on_shutdown: begin fun () ->
  Printf.eprintf "linenum_server shut down\n%!";
  return_unit
end
