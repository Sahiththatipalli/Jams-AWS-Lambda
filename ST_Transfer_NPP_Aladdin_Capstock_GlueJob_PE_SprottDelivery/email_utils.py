import os
import mimetypes
import smtplib
import boto3
from email.message import EmailMessage
from email.utils import formatdate, make_msgid

# ---------------- helpers ---------------- #

def parse_recipients(s: str | None) -> list[str]:
    if not s:
        return []
    parts = [p.strip() for p in s.replace(";", ",").split(",")]
    return [p for p in parts if p]

def _build_mime(subject: str, sender: str, to_addrs: list[str],
                body_text: str | None = None,
                body_html: str | None = None,
                attachments: list[str] | None = None) -> EmailMessage:
    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"] = sender
    msg["To"] = ", ".join(to_addrs)
    msg["Date"] = formatdate(localtime=True)
    msg["Message-ID"] = make_msgid(domain=sender.split("@")[-1])

    if body_html:
        msg.set_content(body_text or "Please see the HTML part of this message.")
        msg.add_alternative(body_html, subtype="html")
    else:
        msg.set_content(body_text or "")

    for path in (attachments or []):
        filename = os.path.basename(path)
        ctype = mimetypes.guess_type(filename)[0] or "application/octet-stream"
        maintype, subtype = ctype.split("/", 1)
        with open(path, "rb") as f:
            msg.add_attachment(f.read(), maintype=maintype, subtype=subtype, filename=filename)
    return msg

def _assume_role_ses_client(region: str):
    """
    If SES_ROLE_ARN is set, assume that role (optionally with SES_EXTERNAL_ID)
    and return a boto3 SES client built with the temporary credentials.
    Otherwise return a normal client in this account/role.
    """
    role_arn = os.getenv("SES_ROLE_ARN")
    if not role_arn:
        return boto3.client("ses", region_name=region)

    external_id = os.getenv("SES_EXTERNAL_ID")
    sts = boto3.client("sts")
    assume_kwargs = {
        "RoleArn": role_arn,
        "RoleSessionName": "lambda-ses-send",
    }
    if external_id:
        assume_kwargs["ExternalId"] = external_id

    creds = sts.assume_role(**assume_kwargs)["Credentials"]

    session = boto3.session.Session(
        aws_access_key_id=creds["AccessKeyId"],
        aws_secret_access_key=creds["SecretAccessKey"],
        aws_session_token=creds["SessionToken"],
        region_name=region,
    )
    return session.client("ses", region_name=region)

# ---------------- SES transport ---------------- #

def send_email_with_attachment(
    ses_region: str,
    sender: str,
    to_addrs: list[str],
    subject: str,
    body_text: str,
    attachment_path: str,
    attachment_name: str | None = None,
) -> str | None:
    """
    Send one attachment via SES.SendRawEmail.
    Honors cross-account settings via env:
      - SES_ROLE_ARN        (required to send from a different account)
      - SES_EXTERNAL_ID     (optional external ID to assume the role)
    """
    if not to_addrs:
        return None
    if not attachment_name:
        attachment_name = os.path.basename(attachment_path)

    msg = _build_mime(subject, sender, to_addrs, body_text=body_text, attachments=[attachment_path])

    ses = _assume_role_ses_client(ses_region)
    resp = ses.send_raw_email(RawMessage={"Data": msg.as_bytes()})
    return resp.get("MessageId")

# ---------------- SMTP transport (unchanged) ---------------- #

def send_email_via_smtp(
    smtp_host: str,
    smtp_port: int,
    sender: str,
    to_addrs: list[str],
    subject: str,
    body_text: str | None = None,
    body_html: str | None = None,
    attachments: list[str] | None = None,
    *,
    use_starttls: bool = False,
    username: str | None = None,
    password: str | None = None,
    helo_host: str | None = None,
    timeout: int = 60,
    dsn_notify: str | None = "SUCCESS,FAILURE",
    dsn_ret: str | None = "HDRS",
) -> str:
    if not to_addrs:
        raise ValueError("No recipients provided")

    msg = _build_mime(subject, sender, to_addrs, body_text=body_text, body_html=body_html, attachments=attachments)
    message_id = msg["Message-ID"]

    if helo_host:
        server = smtplib.SMTP(host=smtp_host, port=smtp_port, local_hostname=helo_host, timeout=timeout)
    else:
        server = smtplib.SMTP(host=smtp_host, port=smtp_port, timeout=timeout)

    try:
        server.ehlo_or_helo_if_needed()
        if use_starttls:
            server.starttls()
            server.ehlo()
        if username and password:
            server.login(username, password)

        mail_opts = []
        rcpt_opts = []
        if dsn_ret:
            mail_opts.append(f"RET={dsn_ret}")
        if dsn_notify:
            rcpt_opts.append(f"NOTIFY={dsn_notify}")

        server.sendmail(sender, to_addrs, msg.as_bytes(), mail_options=mail_opts, rcpt_options=rcpt_opts)
    finally:
        try:
            server.quit()
        except Exception:
            server.close()
    return message_id
