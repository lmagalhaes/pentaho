### Environment dependencies

This project depens on [Python 3.8](https://www.python.org/downloads/release/python-380/) and 
[pipenv](https://pipenv-fork.readthedocs.io/en/latest/).

If you don't have Python3.8 in you environment I would strongly suggest you to use [pyenv](https://pipenv-fork.readthedocs.io/en/latest/)
to manage multiple Python versions without messing up with your original OS python version.

It also integrates greatly with pipenv and allows it to automatically install required Python versions for future projects.


### Installing

To install just clone the repo in your workspace (or whatever folder you want to install the project).

```bash
git clone https://github.com/lmagalhaes/sksnap.git ~/workspace
```

Once the repository is cloned change to the `sksnap` directory and install the dependencies using pipenv.

```bash
cd sksnap
pipenv install 
```

If everything went well you will have a new virtual environment created by pipenv, that should be called `sksnap-[random_string]`.

You can check it by typing the following command: 
```bash
sksnap [ master % ]$ pipenv --venv
~/.local/share/virtualenvs/sksnap-V1uTsCnm
```

That is all.

### How to use

Once everything is installed, you can start toying with the cli.

But, before you start, you have to create a `.env` file, based on the `.env.dist`

```bash
cp .env{.dist,}  # {.dist,} will replace ".dist" string with ""
```

and update the values on the `.env` file with actual values

All the variables inside the `.env` file will be automatically loaded by pipenv and expose the variables to the CLI.

Now, just run `pipenv run sksnap` and you should see the CLI help.

```bash
sksnap [ master ]$ pipenv run sksnap
Usage: sksnap.py [OPTIONS] COMMAND [ARGS]...

Options:
  --help  Show this message and exit.

Commands:
  download  Downloads the original files to be processed.
  process   Process original files downloaded using the upload command.
  upload    Pushes processed files back to s3
```
