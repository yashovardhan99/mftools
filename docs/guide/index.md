# Getting Started

## Installation

### With pip
To get started, install NiveshPy from [PyPi](https://pypi.org/project/NiveshPy/):

```sh
pip install niveshpy
```

### With git
You can also download the app from GitHub and build it on your own.

First, clone the repository:

```sh
git clone https://github.com/yashovardhan99/niveshpy.git
```

Next, install the package and all it's dependencies with:
```sh
pip install -e niveshpy
```

## Quick Start

After installing, you can simply start using NiveshPy:

```py
from niveshpy import Nivesh

app = Nivesh()

app.get_quotes(...)
```

Currently, the following functions are supported:
`app.get_quotes()` to get the current/historical prices of your instruments.
`app.get_tickers()` to get a list of all available tickers.
`app.get_sources()` to check all configured sources.

## Plugins
NiveshPy ships with some pre-built plugins to make your life easier.

To learn more about these plugins, check out our [Plugins page](../plugins/index.md)
