class enum:
  def __init__(self,  *just_names, **variants):
    self.value_to_name = {}
    for (i, name) in enumerate(just_names):
      self.value_to_name[i] = name
      setattr(self, name, i)
      
    for (name, value) in variants.items():
      self.value_to_name[value] = name
      setattr(self, name, value)
  
  def to_str(self, value):
    if value in self.value_to_name:
      return self.value_to_name[value]
    else: raise \
      RuntimeError("No variant found in code %s" % value)