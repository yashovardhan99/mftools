# NiveshPy

<!-- --8<-- [start:common-1] -->

NiveshPy is a financial library designed for managing mutual funds and investment portfolios. It provides functionalities to fetch fund prices, manage portfolios, and calculate performance.

<!-- --8<-- [end:common-1] -->

**Documentation**: [http://yashovardhan99.github.io/niveshpy](http://yashovardhan99.github.io/niveshpy)

NiveshPy is targeted towards Indian :india: markets, but the API is built in a way that any financial data can be added as a source. See [our guide on Plugins](http://yashovardhan99.github.io/niveshpy/plugins/) for more information.

<!-- --8<-- [start:common-2] -->

## Quick Start

To get started, install NiveshPy from [PyPi](https://pypi.org/project/NiveshPy/):

```sh
pip install niveshpy
```

After installing, you can simply start using NiveshPy:

```py
from niveshpy import Nivesh

app = Nivesh()

app.get_quotes(...)
```

## Work in progress

This project is a work in progress. The public API is still in development and unstable.

## License

This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for more details.

<!-- --8<-- [end:common-2] -->