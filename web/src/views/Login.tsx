import { api } from '../api';
import { ArrowRight, Beaker, LockKeyhole, RefreshCw } from 'lucide-react';
import type { ViewProps } from './props';

export function SessionLoading(props: Pick<ViewProps, 'appearance' | 'offline'>) {
  const { appearance, offline } = props;
  return (
    <main className="login">
      <div className="login-appearance">{appearance}</div>
      <div className="login-brand">
        <Beaker size={30} />
        <span>ExperimentTracker</span>
      </div>
      <div className="session-loading" role="status" aria-live="polite">
        <RefreshCw size={24} className={offline ? undefined : 'spinner'} aria-hidden="true" />
        <p>{offline ? 'Unable to reach your workspace' : 'Opening your workspace…'}</p>
        {offline && <small>Check your connection. Retrying automatically…</small>}
      </div>
    </main>
  );
}

export function SignIn(
  props: Pick<
    ViewProps,
    'act' | 'appearance' | 'busy' | 'error' | 'offline' | 'setAuth' | 'setToken' | 'token'
  >,
) {
  const { act, appearance, busy, error, offline, setAuth, setToken, token } = props;
  return (
    <main className="login">
      <div className="login-appearance">{appearance}</div>
      <div className="login-brand">
        <Beaker size={30} />
        <span>ExperimentTracker</span>
      </div>
      <div className="login-card">
        <h1>ExperimentTracker</h1>
        <p>Sign in to your private experiment workspace.</p>
        <form
          onSubmit={(e) => {
            e.preventDefault();
            void act(async () => {
              await api('/api/session', {
                method: 'POST',
                headers: { Authorization: `Bearer ${token}` },
              });
              setToken('');
              setAuth(true);
            });
          }}
        >
          <label htmlFor="token">Access token</label>
          <input
            id="token"
            type="password"
            value={token}
            onChange={(e) => setToken(e.target.value)}
            autoComplete="off"
            required
            placeholder="Your local or gateway access token"
          />
          <button className="primary" disabled={busy}>
            <LockKeyhole size={17} />
            {busy ? 'Signing in…' : 'Open workspace'}
            <ArrowRight size={17} />
          </button>
        </form>
        {error && (
          <p role="alert" className="error">
            {error}
          </p>
        )}
        {offline && (
          <p role="status">
            The service is unreachable. Start it and connect through your SSH tunnel.
          </p>
        )}
        <small>
          Your token stays in a private session cookie. Experiment data is never cached offline.
        </small>
      </div>
    </main>
  );
}
