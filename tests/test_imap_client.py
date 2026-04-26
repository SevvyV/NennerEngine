"""Tests for imap_client credentials, IMAP connection wiring, and the
SPF/DKIM authentication helper.

Process_email atomicity is covered by tests/test_atomicity.py — this file
covers the parts that don't need an open transaction:

  - get_credentials: env-var path, Azure Key Vault path, error path.
  - connect_imap: passes the IMAP_TIMEOUT through and calls login().
  - _email_authenticated: trusts Gmail's Authentication-Results header.
"""

import os
import unittest
from email.message import EmailMessage
from unittest.mock import MagicMock, patch

import sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from nenner_engine.imap_client import (
    get_credentials,
    connect_imap,
    _email_authenticated,
)


class TestGetCredentials(unittest.TestCase):

    def setUp(self):
        # Snapshot env so we can restore it cleanly.
        self._saved = {
            k: os.environ.get(k)
            for k in (
                "GMAIL_ADDRESS", "GMAIL_APP_PASSWORD", "AZURE_KEYVAULT_URL",
                "GMAIL_ADDRESS_SECRET", "GMAIL_PASSWORD_SECRET",
            )
        }
        for k in self._saved:
            os.environ.pop(k, None)

    def tearDown(self):
        for k, v in self._saved.items():
            if v is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = v

    def test_env_vars_take_precedence(self):
        os.environ["GMAIL_ADDRESS"] = "x@example.com"
        os.environ["GMAIL_APP_PASSWORD"] = "abcd efgh ijkl"
        # Even if Key Vault is also configured, env vars win.
        os.environ["AZURE_KEYVAULT_URL"] = "https://example.vault.azure.net/"
        with patch("nenner_engine.imap_client.load_env_once"):
            addr, pw = get_credentials()
        self.assertEqual(addr, "x@example.com")
        self.assertEqual(pw, "abcd efgh ijkl")

    def test_keyvault_used_when_env_missing(self):
        os.environ["AZURE_KEYVAULT_URL"] = "https://example.vault.azure.net/"

        # Construct fake azure modules that get_credentials imports lazily.
        fake_secret_client = MagicMock()
        fake_secret_client.get_secret.side_effect = lambda name: MagicMock(
            value={
                "gmail-address": "kv@example.com",
                "GmailAppPassword": "kvpw1234",
            }[name]
        )
        fake_keyvault_secrets = MagicMock(
            SecretClient=MagicMock(return_value=fake_secret_client),
        )
        fake_identity = MagicMock(DefaultAzureCredential=MagicMock())

        with patch("nenner_engine.imap_client.load_env_once"), \
             patch.dict(
                 "sys.modules",
                 {
                     "azure.identity": fake_identity,
                     "azure.keyvault.secrets": fake_keyvault_secrets,
                 },
             ):
            addr, pw = get_credentials()

        self.assertEqual(addr, "kv@example.com")
        self.assertEqual(pw, "kvpw1234")

    def test_raises_when_nothing_configured(self):
        # No env vars, no vault URL — must fail loudly so the scheduler
        # doesn't silently hang on a None password.
        with patch("nenner_engine.imap_client.load_env_once"):
            with self.assertRaises(ValueError):
                get_credentials()


class TestConnectImap(unittest.TestCase):
    """connect_imap should pass IMAP_TIMEOUT through and call login()."""

    def test_passes_timeout_and_logs_in(self):
        fake_imap = MagicMock()
        with patch(
            "nenner_engine.imap_client.imaplib.IMAP4_SSL",
            return_value=fake_imap,
        ) as ctor, \
             patch("nenner_engine.imap_client.IMAP_TIMEOUT", 30):
            out = connect_imap("user@example.com", "pw")

        # Constructor was called with the timeout kwarg
        _, kwargs = ctor.call_args
        self.assertEqual(kwargs.get("timeout"), 30)
        # Login happened with the supplied credentials
        fake_imap.login.assert_called_once_with("user@example.com", "pw")
        self.assertIs(out, fake_imap)


class TestEmailAuthenticated(unittest.TestCase):
    """_email_authenticated trusts Gmail's Authentication-Results header."""

    def _msg_with_auth(self, auth_value: str | None) -> EmailMessage:
        msg = EmailMessage()
        if auth_value is not None:
            msg["Authentication-Results"] = auth_value
        return msg

    def test_dmarc_pass_accepted(self):
        msg = self._msg_with_auth("mx.google.com; dmarc=pass header.from=charlesnenner.com")
        self.assertTrue(_email_authenticated(msg))

    def test_dkim_pass_with_expected_domain_accepted(self):
        msg = self._msg_with_auth(
            "mx.google.com; dkim=pass header.i=@charlesnenner.com"
        )
        self.assertTrue(_email_authenticated(msg))

    def test_dkim_pass_with_other_domain_rejected(self):
        msg = self._msg_with_auth(
            "mx.google.com; dkim=pass header.i=@spoofer.com"
        )
        self.assertFalse(_email_authenticated(msg))

    def test_missing_header_rejected(self):
        msg = self._msg_with_auth(None)
        self.assertFalse(_email_authenticated(msg))

    def test_failure_marker_rejected(self):
        # Per implementation: the helper looks for explicit pass markers,
        # so any other value (including dmarc=fail) is rejected.
        msg = self._msg_with_auth("mx.google.com; dmarc=fail")
        self.assertFalse(_email_authenticated(msg))


if __name__ == "__main__":
    unittest.main()
