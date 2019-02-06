import ast
import click


class PythonLiteralOption(click.Option):
    """
    allow passing click a list of floats or ints
    https://stackoverflow.com/a/47730333/386279
    """

    def type_cast_value(self, ctx, value):
        try:
            return ast.literal_eval(value)
        except Exception:
            raise click.BadParameter(value)
