from manage import db 

class users (db.Model):
    __tablename__ = "users"
    id = db.Column(db.Integer, primary_key = True, unique = True) 
    email = db.Column(db.String())
    role = db.Column(db.String())
    password = db.Column(db.String())


    def __init__(self, email, role, password):
       self.email = email
       self.role = role
       self.password = password


    def __repr__(self):
        return f""
