"""Allow user to run fileops as a module."""

# Execute with:
# $ python -m glances (3+)

import fileops.cli

if __name__ == '__main__':
    fileops.cli.main()