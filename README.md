# Surfglass

Your best wave doesn't have to be a memory. Browse and buy surf photos from 
local photographers.

## Quick Start

Ensure you have the necessary programs installed:
```bash
gh --version
uv --version
python --version
```

Clone the repository, download the necessary packages, and serve the
documentation site locally.
```bash
gh repo clone calepayson/surfglass
uv sync
uv run zensical serve
```

Then click [here](http://localhost:8000/) to open the documentation site in
your browser.

## Required and Recommended Programs

- [GitHub CLI](https://cli.github.com/) - Recommended - Abstracts a lot of
github-specific rigamarole.
- [uv](https://docs.astral.sh/uv/) - Required - A Python package and project
manager. Makes version control easy.
- [Python](https://www.python.org/doc/) - Required - The bare minimum right
here.

## Getting Started

To get up and running quick, follow the Quick Start guide above. Here we go
into a bit more details.

As in the quick start guide, make sure you have the necessary programs
installed:
```bash
gh --version
uv --version
python --version
```

First we clone the repository onto our local filesystem. The easiest way is
with the GitHub CLI. From the command line, navigate to where you want to
download the directory. For example I want it to be at `projects/surfglass` so
I navigate to `projects`. Then run:
```bash
gh repo clone calepayson/surfglass
```

Next we have to set up our package manager. This will make sure that every
contributor is using the same package versions. Without this versions can get
all out of wack and cause major headaches. We'll be using uv so make sure you
have it installed on your machine then run:
```bash
uv sync
```

This will check if there's a virtual environment, set one up if there isn't,
and then download any missing packages (or switch to the required versions).

Finally we want to get the docsite up and running. The docsite is where all our
documentation goes and is the first place you should check if you have
questions. To do this run:
```bash
uv run zensical serve
```

Then click [here](https://localhost:8000)

If you're curious, let's break down that command. Uv is our package manager.
When we say `uv run` we're saying "Run the next command in our virtual
environment". The cool thing about uv is that you don't even have to activate
the virtual environment! The next command is `zensical serve`. This starts the
docsite and serves it locally on your machine (localhost:8000). If you change
anything in the docs, it'll rebuild the docsite and refresh the page in your
browser. Super useful!
