

class OneToManyDict:
  """Like a dictionary, but every key maps to a 
     set of values and the values map back to their
     key. 
  """
  def __init__(self):
    self.key_to_values = {}
    self.value_to_key = {}


  def get_values(self, k):
    return self.key_to_values[k]  

  def get_key(self, v):
    return self.value_to_key[v]

  def add(self, k, v):
    value_set = self.key_to_values.get(k, set([]))
    value_set.add(v)
    self.key_to_values[k] = value_set 

  def remove_key(self, k):
    # first delete all the reverse mappings
    # from the values back to the key
    for v in self.get_values(k):
      del self.value_to_key[v]

    del self.key_to_values[k]

  def remove_value(self, v):
    k = self.get_key(v)
    value_set = self.key_to_values[k]
    value_set.remove(v)
   
  def has_key(self, k):
    return k in self.key_to_values

  def has_value(self, v):
    return v in self.value_to_key 
