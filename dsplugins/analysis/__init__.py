import click

@click.group()
def analysis():
    """Tools set to do some data analysis and tests"""
    pass


class Package:
    def get_root_group(self):
        return analysis