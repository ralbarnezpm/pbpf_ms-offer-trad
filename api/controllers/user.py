from werkzeug.security import generate_password_hash
from ..auth.authentication import write_token
from ..consts import PRODUCTION_URL
# from ..email.sendinblue import send
from ..models.user import User
from ..extensions import db
import pyshorteners

def update_pass(email, new_pass):
    user = User.query.filter_by(email=email).first()
    if user:
        try:
            user.password_pb = generate_password_hash(new_pass, method='sha256')
            db.session.commit()
            return True, "Contraseña actualizada correctamente"
        except Exception as e:
            print(e)
            db.session.rollback()
            return False, "Error al actualizar la contraseña"
    else:
        return False, "Usuario no encontrado"
    
def generate_url_token(user):
    if user is None:
        return None
    else:
        token=write_token({"id": user.id, "email": user.email, 
                        #    "rol": user.grupo, "permisos_fases": {}
        })

    long_url = f"{PRODUCTION_URL}/user/password-reset?token={token}"
    s = pyshorteners.Shortener()
    
    return s.tinyurl.short(long_url)

def pull_email_content(email):
    user = User.query.filter_by(email=email).first()
    url_token=generate_url_token(user)
    username = f"{user.name_user} {user.last_name}"
    return email_content(username, url_token), user.email

def send_email(email_content, email_usuario):
    # send("Recuperación de Contraseña", email_usuario, email_content)
    print("Recuperación de Contraseña:", email_usuario, email_content)

def pull_user_name_last_name_list():
    """ Pulls user name and last name list """

    users=User.query.all()
    id_and_name_last_name=[(user.id, user.name_user + "_" + user.last_name) for user in users]
    
    return id_and_name_last_name

def email_content(username, recovery_link):
    email_content = f"""<!DOCTYPE html>
                        <html lang="es">
                        <head>
                        <meta charset="UTF-8">
                        <meta http-equiv="X-UA-Compatible" content="IE=edge">
                        <meta name="viewport" content="width=device-width, initial-scale=1.0">
                        <title>Recuperación de Contraseña</title>
                        </head>
                        <body style="font-family: Arial, sans-serif; line-height: 1.6; background-color: #f2f2f2; margin: 0; padding: 0;">
                        <table width="100%" border="0" cellspacing="0" cellpadding="0" style="background-color: #f2f2f2;">
                            <tr>
                            <td align="center" style="padding: 20px 0;">
                                <table width="600" border="0" cellspacing="0" cellpadding="0" style="background-color: #ffffff; border-radius: 8px; box-shadow: 0 4px 8px rgba(0,0,0,0.1); margin: 0 auto;">
                                <tr>
                                    <td align="center" style="padding: 20px;">
                                    <h1 style="color: #3676F5; font-size: 18px; font-family: Montserrat; font-weight: 530; line-height: 22px;">Recuperación de Contraseña</h1>
                                    </td>
                                </tr>
                                <tr>
                                    <td style="padding: 20px; color: #333333; font-size: 16px; line-height: 1.6;">
                                    <p style="color: #2E2E2E; font-size: 15px; font-family: Montserrat; font-weight: 500; line-height: 18px;">¡Hola {username}!</p>
                                    <p style="color: #2E2E2E; font-size: 15px; font-family: Montserrat; font-weight: 500; line-height: 18px; ">Hemos recibido una solicitud para restablecer la contraseña de tu cuenta en PRICEMAKER.</p>
                                    <p style="text-align: center; margin: 30px 0;">
                                        <a href="{recovery_link}" style="display: inline-block; background-color: #007bff; color: #ffffff; text-decoration: none; padding: 12px 24px; border-radius: 4px; font-size: 16px;">Restablecer Contraseña</a>
                                    </p>
                                    
                                                                                        <p style="color: #2E2E2E; font-size: 15px; font-family: Montserrat; font-weight: 500; line-height: 18px; text-align: center;">Si el botón no funciona, copia y pega la siguiente URL en tu navegador:</p>
                                                    <p style="text-align: center; font-size: 14px; margin-top: 10px; background-color: #f7f7f7; padding: 10px; border-radius: 4px;"><span style="word-break: break-all;">{recovery_link}</span></p>
                                    
                                    <p style="margin-top: 24px; text-align: center; color: #2E2E2E; font-size: 16px; font-family: Montserrat; font-weight: 500; line-height: 20px;">Si no realizaste esta solicitud, puedes ignorar este correo.</p>
                                    
                                    <p style="text-align: center; color: #2E2E2E; font-size: 16px; font-family: Montserrat; font-weight: 800; line-height: 20px;">PRICEMAKER</p>
                                    </td>
                                </tr>
                                </table>
                            </td>
                            </tr>
                        </table>
                        </body>
                        </html>"""
    return email_content