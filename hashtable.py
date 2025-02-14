class HashTable:
    def __init__(self):
        self.map = {}
        
    def set(self, key, value):
        self.map[key] = value
    
    def get(self, key):
        if key in self.map:
            return self.map[key]
        else:
            return None