import threading

class DeduplicateWindow:
    def __init__(self, window_size=1000):
        if window_size < 1:
            raise ValueError("window_size tiene que ser mayor a cero")

        self.window_size = window_size
        self.states = {}
        self.lock = threading.Lock()

    def is_duplicate(self, client_id, origin_node, msg_seq):
        
        with self.lock:
            key = (client_id, origin_node)
            state = self.states.get(key)

            if state is None:
                self.states[key] = {
                    "highest": msg_seq,
                    "seen": {msg_seq},
                }
                return False

            highest = state["highest"]

            if msg_seq > highest:
                state["highest"] = msg_seq
                state["seen"].add(msg_seq)
                self.clean_state(state)
                return False

            if msg_seq <= highest - self.window_size:
                return True

            if msg_seq in state["seen"]:
                return True

            state["seen"].add(msg_seq)
            return False

    def clean_state(self, state):
        
        minimum = state["highest"] - self.window_size
        nuevos = set()

        for numero in state["seen"]:
            if numero > minimum:
                nuevos.add(numero)

        state["seen"] = nuevos

    def clear_client(self, client_id):
        
        with self.lock:
            keys = []

            for key in self.states:
                if key[0] == client_id:
                    keys.append(key)

            for key in keys:
                self.states.pop(key, None)