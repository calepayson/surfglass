---
icon: lucide/list-todo
---

# Developer Workflow

Everything you need to know about contributing.

## Working on a feature

Checkout the feature branch.
```bash
git fetch origin
git checkout <feature-branch>
```

Sync your packages.
```bash
uv sync
```

Git commit as you go. Each commit should represent a single atomic function.
Try to keep commits to as few lines as possible. You should rarely ever have a
commit larger than 200 lines (and even this is probably overkill).

When the feature is complete, pull down the most recent version of dev,
merge it into your current branch, and resolve any merge conflicts.
```bash
git checkout dev
git pull
git checkout <feature-branch>
git merge main
```

After merge conflicts have been resolved, push it to the github repository.
```bash
git push
```

From here you can either merge your PR on your own or wait until I do.
