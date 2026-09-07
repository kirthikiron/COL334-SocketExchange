class LineBuffer:
    def __init__(self):
        # We start with an empty byte string. This is our "bucket".
        self.buffer = b""

    def feed(self, data: bytes):
        """
        Appends new bytes to the buffer and extracts all complete, 
        newline-terminated strings. Leaves incomplete data in the buffer.
        """
        self.buffer += data
        messages = []
        
        # Keep slicing as long as there is a newline character in our bucket
        while b'\n' in self.buffer:
            # Split the bucket at the very first newline it finds.
            # 1 means "only split once".
            line_bytes, self.buffer = self.buffer.split(b'\n', 1)
            
            # Decode the complete line from bytes to a standard Python string
            messages.append(line_bytes.decode('utf-8'))
            
        # Return the list of complete messages (could be 0, 1, or many)
        return messages

def send_message(sock, msg: str):
    """
    Safely formats and sends a complete application message over the socket.
    """
    # The protocol requires every message to end with a newline character.
    payload = (msg + "\n").encode('utf-8')
    
    # sendall is a standard Python socket function that loops internally 
    # to guarantee the entire byte sequence is pushed to the OS network stack.
    sock.sendall(payload)