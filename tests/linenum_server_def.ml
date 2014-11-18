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
    try_lwt
      loop 1
    with
    | End_of_file as e -> fail e
    | e ->
        Printf.eprintf "linenum_server internal error: %s\n%!"
          (Printexc.to_string e);
        fail e
end
~on_shutdown: begin fun () ->
  Printf.eprintf "linenum_server shut down\n%!";
  return_unit
end
