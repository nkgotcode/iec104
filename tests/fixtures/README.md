These certificate and key files are public test fixtures for loopback TLS tests.
Never use them as deployment credentials. The certificate includes localhost and
127.0.0.1 subject alternative names and expires in October 2036. Tests keep
certificate and hostname verification enabled with this certificate as an
explicit trust anchor.
