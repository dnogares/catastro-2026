@echo off
echo --- GIT REMOTES --- > git_push_debug.log
git remote -v >> git_push_debug.log 2>&1
echo --- GIT BRANCHES --- >> git_push_debug.log
git branch -vv >> git_push_debug.log 2>&1
echo --- GIT STATUS --- >> git_push_debug.log
git status >> git_push_debug.log 2>&1
echo --- GIT PUSH OUTPUT --- >> git_push_debug.log
git push origin 2026:2026 --force >> git_push_debug.log 2>&1
echo --- GIT PUSH MAIN --- >> git_push_debug.log
git push origin 2026:main --force >> git_push_debug.log 2>&1
