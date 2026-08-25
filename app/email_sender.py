"""SMTP-отправка дайджеста PMAnalyze (Yandex). HTML + plain-fallback, опц. attachment."""
import smtplib
import logging
import re
import ssl
import zipfile
import tempfile
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication
from pathlib import Path
from typing import List, Optional, Dict, Union

logger = logging.getLogger(__name__)


class EmailSender:
    def __init__(self, smtp_host: str, smtp_port: int, smtp_login: str, smtp_password: str):
        self.smtp_host = smtp_host
        self.smtp_port = smtp_port
        self.smtp_login = smtp_login
        self.smtp_password = smtp_password

    def send_digest(
        self,
        to_addresses: List[str],
        subject: str,
        body_html: str,
        attachment_path: Optional[str] = None,
    ) -> Dict[str, str]:
        """Отправить дайджест по email.

        body_html рендерится как HTML (multipart/alternative: plain-fallback + html).
        Если передан attachment_path — файл архивируется в .zip.txt и прикрепляется.
        """
        if not to_addresses:
            return {"status": "error", "message": "Список получателей пуст"}
        if not self.smtp_login or not self.smtp_password:
            return {"status": "error", "message": "SMTP credentials не настроены"}

        try:
            body_text = self._strip_html(body_html)

            # Тело письма: всегда multipart/alternative (plain + html)
            alt = MIMEMultipart("alternative")
            alt.attach(MIMEText(body_text, "plain", "utf-8"))
            alt.attach(MIMEText(body_html, "html", "utf-8"))

            if attachment_path and Path(attachment_path).exists():
                filepath = Path(attachment_path)
                zip_txt_name = filepath.stem + ".zip.txt"
                tmp_zip = Path(tempfile.gettempdir()) / zip_txt_name
                with zipfile.ZipFile(tmp_zip, "w", zipfile.ZIP_DEFLATED) as zipf:
                    zipf.write(filepath, arcname=filepath.name)

                msg = MIMEMultipart("mixed")
                msg.attach(alt)
                with open(tmp_zip, "rb") as f:
                    part = MIMEApplication(f.read(), Name=zip_txt_name)
                part["Content-Disposition"] = f'attachment; filename="{zip_txt_name}"'
                msg.attach(part)
                tmp_zip.unlink(missing_ok=True)
            else:
                msg = alt

            msg["Subject"] = subject
            msg["From"] = self.smtp_login
            msg["To"] = ", ".join(to_addresses)

            self._connect_and_send(msg, to_addresses)
            logger.info(f"Email отправлен: {to_addresses}")
            return {"status": "ok", "sent_to": ", ".join(to_addresses)}

        except smtplib.SMTPDataError as e:
            logger.error(f"SMTP data error (spam?): {e}")
            return {"status": "error", "message": f"Письмо отклонено сервером: {e}"}
        except smtplib.SMTPAuthenticationError as e:
            logger.error(f"SMTP auth error: {e}")
            return {"status": "error", "message": "Ошибка аутентификации SMTP"}
        except smtplib.SMTPRecipientsRefused as e:
            logger.error(f"Recipients refused: {e}")
            return {"status": "error", "message": f"Адреса отклонены: {e}"}
        except (ConnectionError, TimeoutError, OSError) as e:
            logger.error(f"SMTP connection error: {e}")
            return {"status": "error", "message": f"Не удалось подключиться к SMTP: {e}"}
        except Exception as e:
            logger.error(f"Email send error: {e}")
            return {"status": "error", "message": str(e)}

    def _connect_and_send(self, msg: Union[MIMEText, MIMEMultipart], to_addresses: List[str]):
        context = ssl.create_default_context()
        with smtplib.SMTP_SSL(self.smtp_host, self.smtp_port, context=context) as server:
            server.login(self.smtp_login, self.smtp_password)
            server.sendmail(self.smtp_login, to_addresses, msg.as_string())

    def _strip_html(self, html: str) -> str:
        text = re.sub(r'<br\s*/?>', '\n', html)
        text = re.sub(r'</?p>', '\n', text)
        text = re.sub(r'<[^>]+>', '', text)
        text = re.sub(r'\n{3,}', '\n\n', text)
        return text.strip()
