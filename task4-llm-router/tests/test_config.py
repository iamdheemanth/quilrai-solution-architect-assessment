import dataclasses

import pytest

from llm_router.config import ConfigError, Provider, Settings, load_settings

CANARY = "CANARY-SECRET-d41d8cd98f00b204"


def base_env(**overrides: str) -> dict[str, str]:
    env = {
        "ROUTER_TENANT_KEYS": f"key-{CANARY}-a=acme,key-{CANARY}-b=globex",
        "ROUTER_KEY_PEPPER": f"pepper-{CANARY}-0123456789",
        "PRIMARY_BASE_URL": "http://127.0.0.1:9001/v1",
        "PRIMARY_API_KEY": f"pk-{CANARY}",
        "PRIMARY_MODEL": "primary-model",
        "SECONDARY_BASE_URL": "https://backup.example.com/v1",
        "SECONDARY_API_KEY": f"sk-{CANARY}",
        "SECONDARY_MODEL": "secondary-model",
    }
    env.update(overrides)
    return env


def without(env: dict[str, str], name: str) -> dict[str, str]:
    return {k: v for k, v in env.items() if k != name}


def test_defaults():
    s = load_settings(base_env())
    assert s.tenant_keys == {f"key-{CANARY}-a": "acme", f"key-{CANARY}-b": "globex"}
    assert s.tokens_per_minute == 50000
    assert s.window_seconds == 60
    assert s.default_max_tokens == 1024
    assert s.db_path == "./router.db"
    assert s.host == "127.0.0.1"
    assert s.port == 8004
    assert s.log_level == "INFO"
    assert s.primary == Provider(
        name="primary",
        base_url="http://127.0.0.1:9001/v1",
        api_key=f"pk-{CANARY}",
        model="primary-model",
        timeout_s=3.0,
    )
    assert s.secondary.name == "secondary"
    assert s.secondary.base_url == "https://backup.example.com/v1"
    assert s.secondary.timeout_s == 3.0


def test_overrides_are_applied():
    s = load_settings(
        base_env(
            ROUTER_TOKENS_PER_MINUTE="1000",
            ROUTER_WINDOW_SECONDS="10",
            ROUTER_DEFAULT_MAX_TOKENS="256",
            ROUTER_DB_PATH="/tmp/x.db",
            PRIMARY_TIMEOUT_MS="250",
            SECONDARY_TIMEOUT_MS="1500",
            ROUTER_HOST="0.0.0.0",
            ROUTER_PORT="9000",
            LOG_LEVEL="debug",
        )
    )
    assert (s.tokens_per_minute, s.window_seconds, s.default_max_tokens) == (1000, 10, 256)
    assert s.db_path == "/tmp/x.db"
    assert s.primary.timeout_s == 0.25
    assert s.secondary.timeout_s == 1.5
    assert (s.host, s.port, s.log_level) == ("0.0.0.0", 9000, "DEBUG")


def test_tenant_keys_trim_whitespace_and_split_on_first_equals():
    s = load_settings(base_env(ROUTER_TENANT_KEYS="  k1 = acme , k2=glob=ex  "))
    assert s.tenant_keys == {"k1": "acme", "k2": "glob=ex"}


def test_missing_required_variables_are_named():
    env = base_env()
    for name in (
        "ROUTER_TENANT_KEYS",
        "ROUTER_KEY_PEPPER",
        "PRIMARY_BASE_URL",
        "PRIMARY_API_KEY",
        "PRIMARY_MODEL",
        "SECONDARY_BASE_URL",
        "SECONDARY_API_KEY",
        "SECONDARY_MODEL",
    ):
        with pytest.raises(ConfigError, match=name):
            load_settings(without(env, name))
        with pytest.raises(ConfigError, match=name):
            load_settings({**env, name: "   "})


@pytest.mark.parametrize(
    "value",
    [
        f"key-{CANARY}",  # no '='
        f"=acme-{CANARY}",  # empty key
        f"key-{CANARY}=",  # empty tenant
        f"{CANARY}=a,{CANARY}=b",  # duplicate key
        f"{CANARY}=a,,{CANARY}-2=b",  # empty entry
        " , ",
    ],
)
def test_invalid_tenant_keys(value):
    with pytest.raises(ConfigError, match="ROUTER_TENANT_KEYS") as exc:
        load_settings(base_env(ROUTER_TENANT_KEYS=value))
    assert CANARY not in str(exc.value)


def test_short_pepper_rejected_without_leaking_it():
    pepper = f"short-{CANARY[:5]}"
    assert len(pepper.encode()) < 16
    with pytest.raises(ConfigError, match="ROUTER_KEY_PEPPER") as exc:
        load_settings(base_env(ROUTER_KEY_PEPPER=pepper))
    assert pepper not in str(exc.value)


def test_pepper_length_is_counted_in_bytes():
    # 8 characters but 24 bytes in UTF-8
    load_settings(base_env(ROUTER_KEY_PEPPER="€" * 8))
    # exactly 16 ASCII bytes is accepted, 15 is not
    load_settings(base_env(ROUTER_KEY_PEPPER="a" * 16))
    with pytest.raises(ConfigError, match="ROUTER_KEY_PEPPER"):
        load_settings(base_env(ROUTER_KEY_PEPPER="a" * 15))


@pytest.mark.parametrize(
    "name",
    [
        "ROUTER_TOKENS_PER_MINUTE",
        "ROUTER_WINDOW_SECONDS",
        "ROUTER_DEFAULT_MAX_TOKENS",
        "PRIMARY_TIMEOUT_MS",
        "SECONDARY_TIMEOUT_MS",
    ],
)
@pytest.mark.parametrize("value", ["0", "-5", "abc", "1.5", "nan"])
def test_positive_int_variables_reject_bad_values(name, value):
    with pytest.raises(ConfigError, match=name):
        load_settings(base_env(**{name: value}))


@pytest.mark.parametrize("value", ["0", "65536", "-1", "http", "80.5"])
def test_port_range(value):
    with pytest.raises(ConfigError, match="ROUTER_PORT"):
        load_settings(base_env(ROUTER_PORT=value))


def test_port_bounds_accepted():
    assert load_settings(base_env(ROUTER_PORT="1")).port == 1
    assert load_settings(base_env(ROUTER_PORT="65535")).port == 65535


@pytest.mark.parametrize("value", ["verbose", "TRACE", "5", "NOTSET"])
def test_log_level_must_be_standard(value):
    with pytest.raises(ConfigError, match="LOG_LEVEL"):
        load_settings(base_env(LOG_LEVEL=value))


@pytest.mark.parametrize("level", ["debug", "Info", "WARNING", "error", "critical"])
def test_log_level_accepts_standard_names_case_insensitively(level):
    assert load_settings(base_env(LOG_LEVEL=level)).log_level == level.upper()


@pytest.mark.parametrize("prefix", ["PRIMARY", "SECONDARY"])
@pytest.mark.parametrize(
    "url",
    [
        "ftp://example.com/v1",
        "example.com/v1",
        "http://",
        "file:///etc/passwd",
        "http://h:99999/v1",
    ],
)
def test_base_url_validation(prefix, url):
    name = f"{prefix}_BASE_URL"
    with pytest.raises(ConfigError, match=name):
        load_settings(base_env(**{name: url}))


def test_errors_never_contain_secrets():
    cases = [
        {"ROUTER_TENANT_KEYS": f"{CANARY}=a,{CANARY}=b"},
        {"ROUTER_TENANT_KEYS": f"{CANARY}="},
        {"ROUTER_KEY_PEPPER": CANARY[:10]},
        {"PRIMARY_BASE_URL": f"ftp://user:{CANARY}@host/v1"},
        {"SECONDARY_BASE_URL": f"user:{CANARY}@host"},
        {"PRIMARY_TIMEOUT_MS": CANARY},
        {"ROUTER_PORT": CANARY},
        {"LOG_LEVEL": CANARY},
    ]
    for override in cases:
        with pytest.raises(ConfigError) as exc:
            load_settings(base_env(**override))
        assert CANARY not in str(exc.value), override
        assert CANARY[:10] not in str(exc.value), override


def test_repr_hides_keys_and_pepper():
    s = load_settings(base_env())
    text = repr(s) + repr(s.primary) + repr(s.secondary)
    assert CANARY not in text
    assert "primary-model" in text  # non-secret fields are still shown


def test_settings_are_frozen():
    s = load_settings(base_env())
    with pytest.raises(dataclasses.FrozenInstanceError):
        s.port = 1  # type: ignore[misc]
    assert isinstance(s, Settings)


def test_reads_os_environ_when_env_is_none(monkeypatch):
    for k, v in base_env().items():
        monkeypatch.setenv(k, v)
    assert load_settings().primary.model == "primary-model"
