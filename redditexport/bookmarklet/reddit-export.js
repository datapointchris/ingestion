(async () => {
  const exporter = 'reddit-export-bookmarklet/2';
  const pauseMs = 3000;
  const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));
  const label = document.title;
  const progress = (text) => { document.title = 'Reddit export ' + text; };
  const get = async (path) => {
    for (let tryNumber = 1; ; tryNumber++) {
      await sleep(pauseMs);
      const response = await fetch(path, { credentials: 'include' });
      if (response.status === 429 && tryNumber < 5) {
        const reset = Number(response.headers.get('x-ratelimit-reset'));
        await sleep((reset > 0 ? reset : 60) * 1000);
        continue;
      }
      if (!response.ok) {
        const error = new Error(path + ' returned HTTP ' + response.status);
        error.status = response.status;
        throw error;
      }
      return JSON.parse(await response.text(), (key, value) => (key === 'modhash' ? undefined : value));
    }
  };
  const threadPath = (child) => {
    if (child.kind === 't1') {
      return '/comments/' + child.data.link_id.replace(/^t3_/, '') + '/_/' + child.data.id + '.json?context=3&raw_json=1';
    }
    return '/comments/' + child.data.id + '.json?sort=top&limit=500&raw_json=1';
  };
  const withThread = async (child) => {
    try {
      return { child, thread: await get(threadPath(child)) };
    } catch (error) {
      if (error.status === 429) throw error;
      return { child, thread: null, thread_error: error.message };
    }
  };
  const record = { exporter, exported_at: new Date().toISOString(), exported_from: location.href, complete: false };
  const deliver = (name) => {
    document.title = label;
    const link = document.createElement('a');
    link.href = URL.createObjectURL(new Blob([JSON.stringify(record)], { type: 'application/json' }));
    link.download = name;
    document.body.appendChild(link);
    link.click();
    link.remove();
  };
  const day = record.exported_at.slice(0, 10);

  const thread = location.pathname.match(/\/comments\/([a-z0-9]+)/i);
  if (thread) {
    record.kind = 'reddit-thread';
    record.fullname = 't3_' + thread[1];
    try {
      record.listing = await get('/comments/' + thread[1] + '.json?sort=top&limit=500&raw_json=1');
      record.complete = true;
    } catch (error) {
      record.error = error.message;
    }
    deliver('reddit-thread-t3_' + thread[1] + '.json');
    return;
  }

  const choice = prompt('Reddit export: 1 = newest saves, 2 = top posts this month', '1');
  if (choice === '1') {
    const wanted = Number(prompt('How many of the newest saves?', '50')) || 50;
    record.kind = 'reddit-saved';
    record.requested = wanted;
    record.items = [];
    try {
      const me = await get('/api/me.json?raw_json=1');
      record.user = me.data ? me.data.name : me.name;
      let after = null;
      do {
        const page = await get('/user/' + record.user + '/saved.json?limit=100&raw_json=1' + (after ? '&after=' + after : ''));
        for (const child of page.data.children) {
          if (record.items.length >= wanted) break;
          progress((record.items.length + 1) + '/' + wanted);
          record.items.push(await withThread(child));
        }
        after = page.data.after;
      } while (after && record.items.length < wanted);
      record.complete = true;
    } catch (error) {
      record.error = error.message;
    }
    deliver('reddit-saved-' + day + '.json');
  } else if (choice === '2') {
    const stored = localStorage.getItem('reddit-export-subreddits') || '';
    const answer = prompt('Subreddits with a limit each, such as ClaudeCode:50 dataengineering:30', stored);
    if (!answer) return;
    localStorage.setItem('reddit-export-subreddits', answer.trim());
    const wanted = answer.trim().split(/\s+/).map((entry) => {
      const [name, limit] = entry.replace(/^r\//, '').split(':');
      return { name, limit: Number(limit) || 50 };
    });
    record.kind = 'reddit-top';
    record.period = 'month';
    record.subreddits = [];
    try {
      for (const sub of wanted) {
        const listing = await get('/r/' + sub.name + '/top.json?t=month&limit=' + sub.limit + '&raw_json=1');
        const entry = { name: sub.name, limit: sub.limit, listing, threads: {} };
        record.subreddits.push(entry);
        let n = 0;
        for (const child of listing.data.children) {
          progress(sub.name + ' ' + (++n) + '/' + listing.data.children.length);
          const item = await withThread(child);
          entry.threads[child.data.name] = item.thread_error ? { error: item.thread_error } : item.thread;
        }
      }
      record.complete = true;
    } catch (error) {
      record.error = error.message;
    }
    deliver('reddit-top-' + day + '.json');
  }
})().catch((error) => alert('Reddit export failed: ' + error.message));
