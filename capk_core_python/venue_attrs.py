
#global venue_specifics
venue_specifics = {}


class venue_capabilities:

    def __init__(self, venue_id):
        self.venue_id = venue_id
        self.use_synthetic_cancel_replace = False

    def use_synthetic_cancel_replace(self):
        return self.use_synthetic_cancel_replace


