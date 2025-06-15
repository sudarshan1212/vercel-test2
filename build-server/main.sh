#!/bin/bash
echo "🌟 Starting main.sh"

echo "📦 Cloning repo: $GIT_REPOSITORY__URL"
export GIT_REPOSITORY__URL="$GIT_REPOSITORY__URL"

if git clone "$GIT_REPOSITORY__URL" /home/app/output; then
  echo "✅ Git clone completed successfully"
else
  echo "❌ Git clone failed"
  exit 1
fi

echo "🚀 Starting script.js"
exec node script.js
