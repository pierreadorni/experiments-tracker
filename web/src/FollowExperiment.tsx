import { useEffect, useState } from 'react';
import { Bell, BellOff } from 'lucide-react';
import { api, command, RequestError } from './api';

export async function forgetNotificationDevice() {
  if (!('serviceWorker' in navigator)) return;
  const registration = await navigator.serviceWorker.getRegistration();
  const subscription = await registration?.pushManager?.getSubscription();
  if (!subscription) return;
  await api('/api/push/forget', {
    method: 'POST',
    body: JSON.stringify({ endpoint: subscription.endpoint }),
  });
  for (const notice of await registration!.getNotifications()) notice.close();
  await subscription.unsubscribe();
}

function installedOnIOS() {
  const ios =
    /iPad|iPhone|iPod/.test(navigator.userAgent) ||
    (navigator.platform === 'MacIntel' && navigator.maxTouchPoints > 1);
  return (
    !ios ||
    window.matchMedia('(display-mode: standalone)').matches ||
    Boolean((navigator as Navigator & { standalone?: boolean }).standalone)
  );
}
export function FollowExperiment({ id }: { id: string }) {
  const [available, setAvailable] = useState<boolean | null>(null);
  const [publicKey, setPublicKey] = useState('');
  const [following, setFollowing] = useState(false);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState('');
  const [permission, setPermission] = useState(
    typeof Notification === 'undefined' ? 'unsupported' : Notification.permission,
  );
  const supported =
    'serviceWorker' in navigator &&
    'PushManager' in window &&
    'Notification' in window &&
    window.isSecureContext;
  useEffect(() => {
    let active = true;
    setAvailable(null);
    setFollowing(false);
    setError('');
    async function refresh() {
      try {
        const config = await api<{ available: boolean; public_key: string | null }>(
          '/api/push/config',
        );
        if (!active) return;
        setAvailable(config.available);
        setPublicKey(config.public_key || '');
        if (!config.available || !supported) return;
        const registration = await navigator.serviceWorker.ready;
        const subscription = await registration.pushManager.getSubscription();
        if (!subscription) return;
        if (Notification.permission === 'denied') {
          await forgetNotificationDevice();
          return;
        }
        const status = await api<{ experiments: string[] }>('/api/push/status', {
          method: 'POST',
          body: JSON.stringify({ endpoint: subscription.endpoint }),
        });
        if (active) setFollowing(status.experiments.includes(id));
      } catch (e) {
        if (active) {
          if (e instanceof RequestError && e.status === 404) setAvailable(false);
          else {
            setAvailable(false);
            setError('Could not load notification settings. Reopen the experiment to retry.');
          }
        }
      }
    }
    void refresh();
    return () => {
      active = false;
    };
  }, [id, supported]);
  async function toggle() {
    setBusy(true);
    setError('');
    try {
      // Request permission directly from this user gesture, before awaiting network or worker readiness.
      let allowed = Notification.permission;
      if (!following && allowed === 'default') allowed = await Notification.requestPermission();
      setPermission(allowed);
      if (!following && allowed !== 'granted')
        throw new Error('Allow notifications in your browser or phone settings, then try again.');
      const registration = await navigator.serviceWorker.ready;
      let subscription = await registration.pushManager.getSubscription();
      if (!following && !subscription) {
        const key = Uint8Array.from(
          atob(publicKey.replaceAll('-', '+').replaceAll('_', '/')),
          (c) => c.charCodeAt(0),
        );
        subscription = await registration.pushManager.subscribe({
          userVisibleOnly: true,
          applicationServerKey: key,
        });
      }
      if (!subscription) {
        setFollowing(false);
        return;
      }
      const result = await command<{ following: boolean }>(`/api/experiments/${id}/push`, {
        subscription: subscription.toJSON(),
        following: !following,
      });
      setFollowing(result.following);
      if (!result.following)
        for (const notice of await registration.getNotifications({ tag: `experiment:${id}` }))
          notice.close();
    } catch (e) {
      setError(e instanceof Error ? e.message : 'Could not update notifications. Try again.');
    } finally {
      setBusy(false);
    }
  }
  if (available === false && !error) return null;
  const install = !installedOnIOS();
  return (
    <div className="experiment-follow">
      <button
        className="text-button"
        aria-pressed={following}
        disabled={busy || !available || !supported || install}
        onClick={() => void toggle()}
      >
        {following ? <BellOff size={16} /> : <Bell size={16} />}
        {busy
          ? 'Updating notifications…'
          : following
            ? 'Following · stop notifications'
            : 'Follow this experiment'}
      </button>
      <span className="muted small">
        {install
          ? 'On iPhone, add this app to your Home Screen to enable notifications.'
          : !supported
            ? 'Push notifications are unavailable in this browser.'
            : permission === 'denied'
              ? 'Notifications are blocked in your browser settings.'
              : following
                ? 'Stage updates, approvals, failures and ready reports on this device.'
                : 'Receive stage updates and alerts on this device.'}
      </span>
      {error && (
        <p className="notification-error" role="alert">
          {error}
        </p>
      )}
    </div>
  );
}
