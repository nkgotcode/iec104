"""Run the optional independent c104 peer against a dedicated example process."""
import os
import pathlib
import socket
import subprocess
import sys
import tempfile
import time

ROOT = pathlib.Path(__file__).resolve().parents[1]

def main():
    binary = pathlib.Path(sys.argv[1]).resolve()
    with socket.socket() as reservation:
        reservation.bind(('127.0.0.1', 0))
        port = reservation.getsockname()[1]
    env = dict(os.environ, IEC104_HOST='127.0.0.1', IEC104_PORT=str(port),
               RTU_TEST_SECONDS='2', RTU_TEST_REQUIRE_UPDATES='1')
    with tempfile.TemporaryFile(mode='w+') as log:
        server = subprocess.Popen([str(binary)], env=env, stdout=log, stderr=log)
        try:
            deadline = time.monotonic()+5
            while True:
                if server.poll() is not None:
                    raise RuntimeError('Example exited before the peer test')
                try:
                    with socket.create_connection(('127.0.0.1', port), timeout=.1):
                        break
                except OSError:
                    if time.monotonic() >= deadline:
                        raise TimeoutError('Example did not listen')
                    time.sleep(.05)
            result = subprocess.run([sys.executable, str(ROOT/'tests/test_client.py')],
                                    env=env, timeout=90)
            if result.returncode:
                log.seek(0)
                print(log.read()[-10000:], file=sys.stderr)
            return result.returncode
        finally:
            server.terminate()
            try:
                server.wait(timeout=3)
            except subprocess.TimeoutExpired:
                server.kill()
                server.wait(timeout=3)

if __name__ == '__main__':
    sys.exit(main())
