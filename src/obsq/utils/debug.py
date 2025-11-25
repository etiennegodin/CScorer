




def launch_debugger(host="LOCALhost", port=5678, wait=True, max_port_tries=5):
    """
    Robustly start debugpy listener.
    - If a client is already connected, do nothing.
    - Try the requested port, then a few fallback ports, then an ephemeral port.
    - If still failing, re-raise the last error.
    """
    import debugpy
    import socket

    # already connected -> nothing to do
    try:
        if debugpy.is_client_connected():
            print("Debug client already connected; skipping listen()")
            return
    except Exception:
        # older debugpy may not have is_client_connected; ignore
        pass

    last_exc = None
    # try requested port and a few increments
    for attempt in range(max_port_tries):
        try_port = port + attempt
        try:
            debugpy.listen((host, try_port))
            print(f"Debugpy listening on {host}:{try_port}")
            if wait:
                print("Waiting for debugger attach...")
                debugpy.wait_for_client()
            return
        except RuntimeError as e:
            last_exc = e
            print(f"Port {try_port} busy, trying next port...")
            continue

    # fallback: ask OS for an ephemeral port
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.bind((host, 0))
        free_port = s.getsockname()[1]
        s.close()
        debugpy.listen((host, free_port))
        print(f"Debugpy listening on ephemeral port {host}:{free_port}")
        if wait:
            print("Waiting for debugger attach...")
            debugpy.wait_for_client()
        return
    except Exception as e:
        # nothing worked -> raise the original error for visibility
        print("FAILED to start debugpy listener:", e)
        if last_exc:
            raise last_exc
        raise





