from enum import unique
from ..extensions import db


class User(db.Model):
    __tablename__ = 'user_account'
    id = db.Column(db.Integer, primary_key = True)
    name_user = db.Column(db.String(length=20))
    last_name = db.Column(db.String(length=30))
    photo_url =  db.Column(db.String(length=255))
    email =  db.Column(db.String(length=100), unique=True)
    notification = db.Column(db.SmallInteger) 
    active = db.Column(db.SmallInteger) 

    #username = db.Column(db.String(length=50))
    user_rol = db.Column(db.Integer, db.ForeignKey('user_permission.id'), unique=True)
    password_pb = db.Column(db.String(length=150))
    description_user = db.Column(db.TEXT)
    
    permissions = db.relationship("UserPermission", backref=db.backref("users", uselist=False))
    

    def retrieve_data(self):
        return { 
                "id": self.id,
                "name_user": self.name_user,
                "last_name": self.last_name,
                "photo_url": self.photo_url,
                "email": self.email, 
                "notification": self.notification,
                "active": self.active,
                # "password": self.password,
                "rol_and_permissions": self.permissions.retrieve_data(),
                "description_user": self.description_user
            }

    # def __str__(self) -> str:
    #     return f"user with id: {self.id}, email: {self.email}, rol: {self.rol}, users: {self.permissions.retrieve_data()}"
    
    # def __repr__(self) -> str:
    #     return f"<User: {self.names}, {self.surnames}, {self.email}, {self.notifications}, {self.rol}, {self.permissions}>"


class UserPermission(db.Model):
    __tablename__ = 'user_permission'
    id = db.Column(db.Integer, primary_key = True)
    rol = db.Column(db.String(length=50))
    view_promotions = db.Column(db.BINARY)
    create_promotions = db.Column(db.BINARY)
    update_promotions = db.Column(db.BINARY)
    approve_promotions_and_changes = db.Column(db.BINARY)

    def retrieve_data(self):
        return { 
            #"id": self.id, 
            "rol": self.rol,
            "view_promotions": bool(self.view_promotions),
            "create_promotions": bool(self.create_promotions),
            "update_promotions": bool(self.update_promotions),
            "approve_promotions_and_changes": bool(self.approve_promotions_and_changes),
        }
    
    # def __repr__(self) -> str:
    #     return f"""<Rol: {self.rol}, 
    #             {self.view_promotions}, 
    #             {self.create_promotions}, 
    #             {self.update_promotions}, 
    #             {self.approve_promotions_and_changes}>"""
