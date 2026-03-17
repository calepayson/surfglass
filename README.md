# Surfglass

Your best wave doesn't have to be a memory. Browse and buy surf photos from 
local photographers.

## Contributing

### Getting Started

- If you're new to git or it doesn't feel intuitive, read chapters 1-3 of
[ProGit](https://git-scm.com/book/en/v2). You can breaze through it in a few
hours and it'll lay a foundation that will make git intuitive.
- Clone the repository locally.
- You're good to go! (For now)...

### On Issues

We will be tracking everything through github issues. If you want to work on a
bug, a feature, documentation, etc. the first step is to find or create an
associated issue on github.

Every issue should be associated with a branch. The easiest way to do this is
by navigating to the issue and, in the right bar under 'Development', clicking
'create a branch'. Obviously don't do this if there is already a branch
associated with it.

To work on an issue switch to the relevant branch on your local machine. The
typical workflow is:
```bash
git fetch origin
git checkout <branch-name>
```

If you have issues, walk through them with an LLM before you just start merging
everything. If you're using an LLM, make sure it's not going crazy with merges
and branches.

### Pull Requests

Pull requests should merge into the dev branch (not main). This way main should
always be functional and we can revert back to it in the case of some really
catastrophic mistake.

### Dependency Management

We use uv to track the project dependencies. Make sure uv is installed on your
system ([instructions here]()). Then run:

```bash
uv sync
```

This will sync your virtual environment with the one saved on the branch.
