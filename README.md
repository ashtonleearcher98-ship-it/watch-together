# Watch Together — upload once

The host uploads one MP4. Friends open the invite link and watch the same uploaded
video without selecting a local file. Playback, seeking, buffering, chat and
participant status synchronize through Socket.IO.

## Deploy on Render

1. Upload this repository to GitHub. Keep `package.json`, `package-lock.json` and
   `server.js` in the repository root and `index.html` inside `public`.
2. On Render, choose **New → Web Service**, connect GitHub and select the repository.
3. Choose **Node**, branch **main**, leave **Root Directory** blank, and select **Free**.
4. Set **Build Command** to `npm ci` and **Start Command** to `npm start`.
5. Deploy and open the service's HTTPS URL. No environment variables or external
   storage accounts are required. Render supplies `PORT` automatically.

For an existing installation, replace the old files, add `package-lock.json`,
commit changes, and let Render redeploy. The existing `npm install` build command
also works; `npm ci` installs the versions in the included lockfile.

## Watching

Create a room, upload an MP4, and send the invite link to your friends. Guests join
and load the video automatically. The host controls play, pause and seeking.
If prompted, each viewer clicks **Click to enable playback** once to permit audio.
Use browser-compatible H.264 video and AAC audio inside an MP4 container. This
server does not transcode unsupported codecs.

Keep at least one participant in the room. The video is deleted when the last
participant leaves. A disconnect can transfer host controls to another viewer.

## Limits and temporary storage

- One uploaded video per room, up to **1 GiB** per file.
- **2 GiB** total temporary storage, including in-flight uploads and replacements.
- Up to 12 people per room; 500 rooms maximum.
- Files stream to disk during upload and use HTTP byte ranges during playback.
  The server does not buffer entire movies in memory.
- Videos and rooms are temporary. Render restarts/redeploys/idle shutdowns erase
  them; create a new room and upload again. There is no app session timer.
- Serving movies consumes Render outbound bandwidth for every viewer. Render
  Free has monthly quotas; this is not unlimited free video hosting. Check usage
  in the Render dashboard. Hosting availability and disk capacity are controlled
  by Render, not this app.
- Upload credentials are host-only. Video URLs contain unpredictable access keys;
  anyone with a complete video URL can use it while the room exists. The app has
  no accounts or permanent media library.
- This is a small private-party app, not a hardened public upload platform.
- Tailwind's browser CDN is retained from the original brief. Compile and
  self-host CSS for a hardened production deployment.

## Run locally

Requires Node.js 22 or newer:

```sh
npm ci
npm start
```

Open http://localhost:3000. Internet access for friends requires deploying the
server or correctly exposing it through HTTPS; localhost is only your computer.
