class enum:
  def __init__(self,  **variants):
    self.value_to_name = {}
    for (name, value) in variants.items():
      self.value_to_name[value] = name
      setattr(self, name, value)
  
  def to_str(self, value):
    if num in self.value_to_name:
      return self.value_to_name[value]
    else raise \
      RuntimeError("No variant found in code %s" % value)