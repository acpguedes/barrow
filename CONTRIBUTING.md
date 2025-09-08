# Contributing

## Environment Setup

Install the development dependencies and configure pre-commit hooks using the Makefile:

```bash
make install
```

## Development Workflow

Run the linters and formatters:

```bash
make lint
make format
```

Run the test suite:

```bash
make test
```

Remove generated files:

```bash
make clean
```

## Versioning

This project follows [Semantic Versioning](https://semver.org/), using
`MAJOR.MINOR.PATCH` numbers to indicate backward-incompatible changes, new
features, and bug fixes respectively. Releases and the changelog are managed
with [Commitizen](https://commitizen-tools.github.io/commitizen/). When you're
ready to cut a new release, run:

```bash
cz bump
```

This command updates version numbers and the `CHANGELOG.md` based on commit
messages that follow the Conventional Commits specification.
