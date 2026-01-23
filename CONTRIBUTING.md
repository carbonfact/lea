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

Next, install [uv](https://docs.astral.sh/uv/getting-started/installation/) and set up the environment:

```sh
uv sync
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
