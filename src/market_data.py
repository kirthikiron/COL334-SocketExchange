import socket
import sys
from protocol import LineBuffer, send_message

def main():
    # The evaluation script passes host, port, and instrument
    if len(sys.argv) != 4:
        print("Usage: python3 market_data.py <host> <port> <instrument>", file=sys.stderr)
        sys.exit(1)
        
    host = sys.argv[1]
    port = int(sys.argv[2])
    instrument = sys.argv[3]
    
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect((host, port))
    
    buffer = LineBuffer()
    
    # Immediately establish our role as a Market-Data client
    send_message(sock, f"SUBSCRIBE {instrument}")
    
    try:
        while True:
            data = sock.recv(4096)
            if not data:
                print("Disconnected from server.", file=sys.stderr)
                break
            
            messages = buffer.feed(data)
            for msg in messages:
                # Print the exact server message to standard output
                print(msg)
                # Force flush to ensure the automated grading script sees the output instantly
                sys.stdout.flush()
                
    except KeyboardInterrupt:
        print("\nExiting market data client...", file=sys.stderr)
    finally:
        sock.close()

if __name__ == "__main__":
    main()