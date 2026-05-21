# Step 05: GitHub Backup

## Purpose

Back up the local `emr_migration_demo/` folder to GitHub so the work can be resumed after a cluster termination or from a new environment.

## Repository

```text
https://github.com/jimsapbw/mastering-emr
```

## Local Clone Used For Backup

The current working folder `/home/hadoop/mastering-emr` was not a Git repository, so a separate clone was created:

```text
/mnt/tmp/mastering-emr-git
```

## Install Git

Git was not installed on the cluster, so it was installed with:

```bash
sudo dnf install -y git
```

## Clone Repository

```bash
git clone https://github.com/jimsapbw/mastering-emr.git /mnt/tmp/mastering-emr-git
```

## Copy Demo Folder

From `/home/hadoop/mastering-emr`:

```bash
cp -a emr_migration_demo /mnt/tmp/mastering-emr-git/
```

## Stage Changes

From `/mnt/tmp/mastering-emr-git`:

```bash
git add emr_migration_demo
```

## Staged Files

```text
emr_migration_demo/KT/00_resume_plan.md
emr_migration_demo/KT/01_create_s3_prefixes.md
emr_migration_demo/KT/02_mock_dataset_generation.md
emr_migration_demo/KT/03_maven_setup.md
emr_migration_demo/KT/04_dataset_contract_and_validation.md
emr_migration_demo/KT/README.md
emr_migration_demo/README.md
emr_migration_demo/scripts/create_s3_prefixes.sh
emr_migration_demo/scripts/generate_mock_data.py
emr_migration_demo/scripts/validate_mock_data.py
```

## Configure Local Git Author

The first commit attempt failed because Git author identity was not configured.

Configured locally in the cloned repository:

```bash
git config user.name jimsapbw
git config user.email jimsapbw@users.noreply.github.com
```

## Commit

```bash
git commit -m "Add EMR migration demo scaffold"
```

Created commit:

```text
eed4b8c Add EMR migration demo scaffold
```

## Push With Token

GitHub HTTPS push required a personal access token.

Safe flow:

```bash
cd /mnt/tmp/mastering-emr-git
read -s GH_TOKEN
```

Paste the token and press Enter. Then test access:

```bash
git ls-remote https://jimsapbw:${GH_TOKEN}@github.com/jimsapbw/mastering-emr.git main
```

Observed remote before push:

```text
d191760cc62eb238e3f49612240abb1b3efce5b3        refs/heads/main
```

Push:

```bash
git push https://jimsapbw:${GH_TOKEN}@github.com/jimsapbw/mastering-emr.git main
unset GH_TOKEN
```

Observed successful push:

```text
d191760..eed4b8c  main -> main
```

## Refresh Local Tracking Branch

After pushing to a direct HTTPS URL, local `origin/main` was not refreshed automatically. `git status` temporarily showed:

```text
Your branch is ahead of 'origin/main' by 1 commit.
```

The fix:

```bash
git fetch origin
git status
```

Final observed status:

```text
On branch main
Your branch is up to date with 'origin/main'.

nothing to commit, working tree clean
```

## GitHub Location

```text
https://github.com/jimsapbw/mastering-emr/tree/main/emr_migration_demo
```

## Security Note

If a GitHub token is pasted into chat, command history, or any visible location, revoke it immediately and create a new token.

Token-safe command pattern:

```bash
read -s GH_TOKEN
git push https://jimsapbw:${GH_TOKEN}@github.com/jimsapbw/mastering-emr.git main
unset GH_TOKEN
```
