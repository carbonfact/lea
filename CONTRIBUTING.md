# Contributing

## Setup

Start by cloning the repository:

```sh
git clone https://github.com/carbonfact/lea
```

There are submodules in this repository, so you'll need to fetch/update them:

```sh
git submodule init
git submodule update
```

Next, you'll need a Python environment:

```sh
pyenv install -v 3.11
```

You'll also need [Poetry](https://python-poetry.org/):

```sh
curl -sSL https://install.python-poetry.org | python3 -
poetry install
poetry shell
```

## Testing

You can run tests once the environment is set up:

```sh
pytest
```

## Code quality

Install the code quality routine so that it runs each time you try to push your commits.

```sh
pre-commit install --hook-type pre-push
```

You can also run the code quality routine ad-hoc.

```sh
pre-commit run --all-files
```
