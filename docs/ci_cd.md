# CI/CD

GitHub Actions workflow: `.github/workflows/ci.yml`.

It runs on pushes and pull requests to `main`/`master`:

- installs Python 3.11 dependencies from `requirements-dev.txt`;
- runs `ruff check .`;
- runs `pytest`;
- blocks PR merge when configured as a required status check in GitHub branch protection.

Docker image build is present but disabled by default. Run the workflow manually with
`build_images=true` to build the API and model-service images without starting containers.

To require tests before merge in GitHub:

1. Open repository settings.
2. Go to `Rules` or `Branches`.
3. Add a rule for the protected branch.
4. Enable required status checks.
5. Select `Ruff` and `Tests`.
