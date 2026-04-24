from pathlib import Path

from lakesource.water_balance import (
    build_storage_report,
    download_gleam_datasets,
    download_grace_dataset,
    ensure_dataset_directories,
    format_bytes,
    GraceAuthConfig,
    GleamAuthConfig,
    load_auth_config,
    remaining_public_download_bytes,
)


def test_format_bytes_uses_binary_units() -> None:
    assert format_bytes(1024) == "1.00 KiB"
    assert format_bytes(1024 * 1024) == "1.00 MiB"


def test_ensure_dataset_directories_creates_expected_targets(tmp_path: Path) -> None:
    ensure_dataset_directories(tmp_path)

    assert (tmp_path / "GPCC").exists()
    assert (tmp_path / "GLEAM").exists()
    assert (tmp_path / "GRACE").exists()


def test_remaining_public_download_bytes_respects_existing_files(
    tmp_path: Path,
) -> None:
    ensure_dataset_directories(tmp_path)
    total_before = remaining_public_download_bytes(tmp_path)

    existing_file = (
        tmp_path
        / "GPCC/full_data_monthly_v2022/025/full_data_monthly_v2022_2001_2010_025.nc.gz"
    )
    existing_file.parent.mkdir(parents=True, exist_ok=True)
    existing_file.write_bytes(b"existing")

    total_after = remaining_public_download_bytes(tmp_path)

    assert total_after < total_before


def test_build_storage_report_is_self_consistent(tmp_path: Path) -> None:
    report = build_storage_report(tmp_path)

    assert report.warehouse_root == tmp_path
    assert report.total_bytes >= report.used_bytes
    assert report.total_bytes >= report.free_bytes
    assert report.recommended_total_bytes >= report.required_public_bytes


def test_load_auth_config_reads_trimmed_values() -> None:
    auth_config = load_auth_config(
        {
            "EARTHDATA_USERNAME": " user ",
            "EARTHDATA_PASSWORD": " secret ",
            "GLEAM_SFTP_USERNAME": " gleam_user ",
            "GLEAM_SFTP_PASSWORD": " gleam_pass ",
            "GLEAM_REMOTE_DIR": " /data/monthly ",
            "GLEAM_REMOTE_FILES": "one.nc, two.nc ",
        }
    )

    assert auth_config.grace.username == "user"
    assert auth_config.grace.password == "secret"
    assert auth_config.grace.ready
    assert auth_config.gleam.remote_dir == "/data/monthly"
    assert auth_config.gleam.remote_files == ("one.nc", "two.nc")


def test_download_grace_dataset_builds_expected_command(
    monkeypatch,
    tmp_path: Path,
) -> None:
    calls: list[list[str]] = []

    def fake_run(command: list[str], check: bool) -> None:
        assert check is True
        calls.append(command)
        target_index = command.index("-o") + 1
        Path(command[target_index]).write_bytes(b"grace")

    monkeypatch.setattr("subprocess.run", fake_run)

    target_path = download_grace_dataset(
        tmp_path,
        GraceAuthConfig(
            username="earth_user",
            password="earth_pass",
            url="https://example.test/path/GRACE.nc",
        ),
    )

    assert target_path.name == "GRACE.nc"
    assert calls
    assert calls[0][0] == "curl"
    assert "--netrc-file" in calls[0]


def test_download_gleam_datasets_builds_expected_commands(
    monkeypatch,
    tmp_path: Path,
) -> None:
    calls: list[list[str]] = []

    def fake_run(command: list[str], check: bool) -> None:
        assert check is True
        calls.append(command)
        target_index = command.index("-o") + 1
        Path(command[target_index]).write_bytes(b"gleam")

    monkeypatch.setattr("subprocess.run", fake_run)

    downloaded_paths = download_gleam_datasets(
        tmp_path,
        GleamAuthConfig(
            host="sftp.gleam.eu",
            port=2225,
            username="gleam_user",
            password="gleam_pass",
            remote_dir="/data/v3.8a/monthly",
            remote_files=("E_1980-2023_GLEAM_v3.8a_MO.nc",),
        ),
    )

    assert len(downloaded_paths) == 1
    assert downloaded_paths[0].name == "E_1980-2023_GLEAM_v3.8a_MO.nc"
    assert calls
    assert calls[0][0] == "curl"
    assert calls[0][-1].startswith("sftp://sftp.gleam.eu:2225/")