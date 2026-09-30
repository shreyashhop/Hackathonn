import React, { useState, useEffect, useCallback } from 'react';
import { motion, AnimatePresence } from 'framer-motion';
import {
  Scale,
  RefreshCw,
  Play,
  XCircle,
  PlusCircle,
  Trash2,
  CheckCircle2,
  AlertTriangle,
  ArrowRight,
  Server,
  Layers,
  Database,
  Activity,
  ShieldCheck,
  HardDrive,
  Clock,
  X,
  Radio,
} from 'lucide-react';

function formatBytes(bytes) {
  if (bytes === 0) return '0 B';
  if (!bytes) return '—';
  const k = 1024;
  const sizes = ['B', 'KB', 'MB', 'GB', 'TB'];
  const i = Math.floor(Math.log(bytes) / Math.log(k));
  return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i];
}

function getRebalanceBadge(status) {
  const s = (status || '').toUpperCase();
  if (s === 'COMPLETED') return { className: 'healthy', label: 'COMPLETED' };
  if (s === 'RUNNING') return { className: 'recovering', label: 'RUNNING' };
  if (s === 'PLANNED') return { className: 'warning', label: 'PLANNED' };
  if (s === 'FAILED') return { className: 'failure', label: 'FAILED' };
  if (s === 'CANCELLED') return { className: 'warning', label: 'CANCELLED' };
  return { className: 'healthy', label: 'IDLE' };
}

function getNodeStatusBadge(status) {
  const s = (status || '').toUpperCase();
  if (s === 'HEALTHY') return { className: 'healthy', label: 'HEALTHY', border: '#10B981' };
  if (s === 'SUSPECT' || s === 'WARNING') return { className: 'warning', label: 'SUSPECT', border: '#F59E0B' };
  if (s === 'DOWN' || s === 'OFFLINE') return { className: 'failure', label: 'DOWN', border: '#EF4444' };
  if (s === 'PARTITIONED') return { className: 'partitioned', label: 'PARTITIONED', border: '#7C3AED' };
  if (s === 'RECOVERING') return { className: 'recovering', label: 'RECOVERING', border: '#2563EB' };
  if (s === 'DECOMMISSIONING') return { className: 'warning', label: 'DECOMMISSIONING', border: '#F59E0B' };
  if (s === 'DECOMMISSIONED') return { className: 'warning', label: 'DECOMMISSIONED', border: '#94A3B8' };
  return { className: 'warning', label: s || 'UNKNOWN', border: '#F59E0B' };
}

function getReasonBadge(reason) {
  const r = (reason || '').toUpperCase();
  if (r.includes('DECOM')) {
    return { label: 'NODE_DECOMMISSIONED', bg: '#FFFBEB', color: '#D97706', border: '#FDE68A' };
  }
  if (r.includes('ADD')) {
    return { label: 'NODE_ADDED', bg: '#EFF6FF', color: '#2563EB', border: '#BFDBFE' };
  }
  return { label: r || 'PLACEMENT_CHANGED', bg: '#F5F3FF', color: '#7C3AED', border: '#DDD6FE' };
}

export default function RebalanceView({ coordinatorBaseUrl = '', nodes = [], onRefreshNodes }) {
  // Explicit state model separating current preview plan from active & last completed execution runs
  const [currentPlan, setCurrentPlan] = useState(null);
  const [activeRun, setActiveRun] = useState(null);
  const [lastCompletedRun, setLastCompletedRun] = useState(null);
  const [loadingPlan, setLoadingPlan] = useState(false);
  const [actionLoading, setActionLoading] = useState(false);
  const [error, setError] = useState(null);
  const [successMsg, setSuccessMsg] = useState(null);

  // Register modal state
  const [showRegisterModal, setShowRegisterModal] = useState(false);
  const [newNodeId, setNewNodeId] = useState('');
  const [newHost, setNewHost] = useState('localhost');
  const [newPort, setNewPort] = useState('8006');
  const [newUrl, setNewUrl] = useState('');

  // Decommission modal state
  const [showDecommissionModal, setShowDecommissionModal] = useState(false);
  const [decomNodeId, setDecomNodeId] = useState('');

  const getBaseUrl = useCallback(() => {
    return coordinatorBaseUrl || (window.location.port === '8000' ? '' : 'http://localhost:8000');
  }, [coordinatorBaseUrl]);

  const apiRequest = useCallback(async (path, options = {}) => {
    const base = getBaseUrl();
    // 1. If a distinct base URL exists, try it first
    if (base) {
      try {
        const res = await fetch(`${base}${path}`, options);
        const ct = res.headers.get('content-type') || '';
        if (res.ok && ct.includes('application/json')) {
          return res;
        }
      } catch (err) {
        console.warn(`Direct fetch to ${base}${path} failed, attempting proxy fallback:`, err.message);
      }
    }

    // 2. Relative reverse proxy fallback (works through Nginx or Vite dev server proxy)
    const res = await fetch(path, options);
    return res;
  }, [getBaseUrl]);

  const fetchPlan = useCallback(async (isManual = false) => {
    try {
      setLoadingPlan(true);
      setError(null);
      const res = await apiRequest('/rebalance/plan');
      if (!res.ok) {
        const errData = await res.json().catch(() => ({}));
        throw new Error(errData.detail || `HTTP ${res.status}`);
      }
      const ct = res.headers.get('content-type') || '';
      if (!ct.includes('application/json')) {
        throw new Error('Received non-JSON response from server');
      }
      const data = await res.json();
      setCurrentPlan(data);
      if (isManual) {
        if (data.total_migrations === 0) {
          setSuccessMsg('Rebalance plan preview: No migrations required (cluster is in optimal balance).');
        } else {
          setSuccessMsg(`Rebalance plan preview: ${data.total_migrations} migration(s) required across ${data.affected_objects} object(s).`);
        }
      }
    } catch (err) {
      console.error('Failed to generate rebalance plan:', err);
      setError(`Failed to generate plan: ${err.message}`);
    } finally {
      setLoadingPlan(false);
    }
  }, [apiRequest]);

  const fetchStatus = useCallback(async () => {
    try {
      const res = await apiRequest('/rebalance/status');
      if (res && res.ok) {
        const ct = res.headers.get('content-type') || '';
        if (ct.includes('application/json')) {
          const data = await res.json();
          if (data && data.status === 'RUNNING') {
            setActiveRun(data);
          } else {
            setActiveRun((prevActive) => {
              // If an active run just finished, refresh current plan to show new balanced topology
              if (prevActive !== null) {
                setTimeout(() => fetchPlan(false), 250);
              }
              return null;
            });
            if (data && data.run_id) {
              setLastCompletedRun(data);
            }
          }
        }
      }
    } catch (err) {
      console.warn('Failed to fetch rebalance status:', err);
    }
  }, [apiRequest, fetchPlan]);

  useEffect(() => {
    fetchStatus();
    fetchPlan(false);
    const interval = setInterval(fetchStatus, 3000);
    return () => clearInterval(interval);
  }, [fetchStatus, fetchPlan]);

  const handleStartRebalance = async () => {
    try {
      setActionLoading(true);
      setError(null);
      setSuccessMsg(null);
      const res = await apiRequest('/rebalance/start', {
        method: 'POST',
      });

      if (!res.ok) {
        const errData = await res.json().catch(() => ({}));
        throw new Error(errData.detail || `HTTP ${res.status}`);
      }
      const data = await res.json();
      setSuccessMsg(data.message || `Rebalancing initiated (${data.total_migrations} migrations).`);
      await fetchStatus();
      if (onRefreshNodes) onRefreshNodes();
    } catch (err) {
      setError(err.message);
    } finally {
      setActionLoading(false);
    }
  };

  const handleCancelRebalance = async () => {
    try {
      setActionLoading(true);
      const res = await apiRequest('/rebalance/cancel', {
        method: 'POST',
      });
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
      setSuccessMsg('Rebalancing cancellation requested.');
      await fetchStatus();
      await fetchPlan(false);
    } catch (err) {
      setError(err.message);
    } finally {
      setActionLoading(false);
    }
  };

  const handleRegisterNode = async (e) => {
    e.preventDefault();
    if (!newNodeId.trim()) {
      setError('Node ID is required');
      return;
    }
    try {
      setActionLoading(true);
      setError(null);
      const payload = {
        node_id: newNodeId.trim(),
        host: newHost.trim() || 'localhost',
        port: parseInt(newPort, 10) || 8006,
      };
      if (newUrl.trim()) payload.url = newUrl.trim();

      const res = await apiRequest('/rebalance/nodes/register', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(payload),
      });

      if (!res.ok) {
        const errData = await res.json().catch(() => ({}));
        throw new Error(errData.detail || `HTTP ${res.status}`);
      }
      setSuccessMsg(`Node "${newNodeId}" registered successfully into cluster topology.`);
      setShowRegisterModal(false);
      setNewNodeId('');
      if (onRefreshNodes) onRefreshNodes();
      await fetchPlan(false);
      await fetchStatus();
    } catch (err) {
      setError(err.message);
    } finally {
      setActionLoading(false);
    }
  };

  const handleDecommissionNode = async (e) => {
    e.preventDefault();
    if (!decomNodeId.trim()) {
      setError('Please select a node to decommission');
      return;
    }
    try {
      setActionLoading(true);
      setError(null);
      const res = await apiRequest('/rebalance/nodes/decommission', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ node_id: decomNodeId.trim() }),
      });

      if (!res.ok) {
        const errData = await res.json().catch(() => ({}));
        throw new Error(errData.detail || `HTTP ${res.status}`);
      }
      setSuccessMsg(`Node "${decomNodeId}" safely marked for decommissioning & replica migration.`);
      setShowDecommissionModal(false);
      setDecomNodeId('');
      if (onRefreshNodes) onRefreshNodes();
      await fetchPlan(false);
      await fetchStatus();
    } catch (err) {
      setError(err.message);
    } finally {
      setActionLoading(false);
    }
  };

  const isRunning = activeRun !== null && activeRun.status === 'RUNNING';

  // Overall Rebalance Status Badge for Header
  const rebalanceBadge = isRunning
    ? { className: 'recovering', label: 'RUNNING' }
    : currentPlan
    ? currentPlan.total_migrations === 0
      ? { className: 'healthy', label: 'BALANCED' }
      : { className: 'warning', label: `${currentPlan.total_migrations} MIGRATIONS PENDING` }
    : lastCompletedRun
    ? getRebalanceBadge(lastCompletedRun.status)
    : { className: 'healthy', label: 'IDLE' };

  // Current Plan vs Active Execution Metrics
  const progressPercent = isRunning
    ? Math.min(100, Math.max(0, activeRun.progress_percent || 0))
    : 0;

  const totalMigrations = isRunning
    ? (activeRun.total_migrations || 0)
    : (currentPlan?.total_migrations || 0);

  const completedMigrations = isRunning
    ? (activeRun.completed_migrations || 0)
    : 0;

  const failedMigrations = isRunning
    ? (activeRun.failed_migrations || 0)
    : 0;

  const bytesTransferred = isRunning
    ? (activeRun.bytes_transferred || 0)
    : 0;

  const bytesTotal = isRunning
    ? (activeRun.bytes_total || 0)
    : (currentPlan?.bytes_total || 0);

  const affectedObjects = isRunning
    ? (activeRun.total_objects || currentPlan?.affected_objects || 0)
    : (currentPlan?.affected_objects || 0);

  const totalObjects = currentPlan?.total_objects ?? lastCompletedRun?.total_objects ?? 0;

  const healthyNodesCount = nodes.filter((n) => (n.status || '').toUpperCase() === 'HEALTHY').length;

  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: '24px', width: '100%' }}>
      {/* 1. PAGE HEADER BANNER */}
      <motion.div
        initial={{ opacity: 0, y: -10 }}
        animate={{ opacity: 1, y: 0 }}
        className="panel"
        style={{
          padding: '28px 32px',
          background: 'linear-gradient(135deg, #FFFFFF 0%, #F8FAFC 100%)',
          borderRadius: 'var(--radius-lg)',
          position: 'relative',
          overflow: 'hidden',
        }}
      >
        {/* Top 4px decorative gradient line */}
        <div
          style={{
            position: 'absolute',
            top: 0,
            left: 0,
            right: 0,
            height: '4px',
            background: 'linear-gradient(90deg, #2563EB 0%, #7C3AED 50%, #06B6D4 100%)',
          }}
        />

        <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start', flexWrap: 'wrap', gap: '20px' }}>
          <div>
            <div style={{ display: 'flex', alignItems: 'center', gap: '14px' }}>
              <div
                style={{
                  width: '44px',
                  height: '44px',
                  borderRadius: 'var(--radius-sm)',
                  background: 'linear-gradient(135deg, #EFF6FF, #F5F3FF)',
                  border: '1px solid #DBEAFE',
                  display: 'flex',
                  alignItems: 'center',
                  justifyContent: 'center',
                  color: '#2563EB',
                  boxShadow: 'var(--shadow-xs)',
                }}
              >
                <Scale size={24} />
              </div>
              <div>
                <div style={{ display: 'flex', alignItems: 'center', gap: '12px' }}>
                  <h2 style={{ fontSize: '1.65rem', fontWeight: 800, color: '#172033', letterSpacing: '-0.02em' }}>
                    Dynamic Data Rebalancing
                  </h2>
                  <span className={`status-pill ${rebalanceBadge.className}`}>
                    <span className="pulse-dot" />
                    {rebalanceBadge.label}
                  </span>
                </div>
                <p style={{ fontSize: '0.88rem', color: '#64748B', marginTop: '4px' }}>
                  Deterministic Rendezvous Hashing (HRW) rebalancing, bounded migration concurrency, and copy-before-delete safety.
                </p>
              </div>
            </div>
          </div>

          {/* Action Buttons Aligned Horizontally */}
          <div style={{ display: 'flex', alignItems: 'center', flexWrap: 'wrap', gap: '10px' }}>
            <button
              onClick={() => setShowRegisterModal(true)}
              className="btn btn-secondary"
              style={{ padding: '8px 16px', fontSize: '0.82rem' }}
            >
              <PlusCircle size={15} />
              <span>Register Node</span>
            </button>

            <button
              onClick={() => setShowDecommissionModal(true)}
              className="btn btn-secondary"
              style={{ padding: '8px 16px', fontSize: '0.82rem' }}
            >
              <Trash2 size={15} />
              <span>Decommission Node</span>
            </button>

            <button
              id="btn-preview-plan"
              type="button"
              onClick={() => fetchPlan(true)}
              disabled={loadingPlan}
              className="btn btn-secondary"
              style={{
                padding: '8px 16px',
                fontSize: '0.82rem',
                cursor: loadingPlan ? 'not-allowed' : 'pointer',
              }}
            >
              <RefreshCw size={15} className={loadingPlan ? 'spin' : ''} />
              <span>{loadingPlan ? 'Generating Plan...' : 'Preview Plan'}</span>
            </button>

            {isRunning ? (
              <button
                onClick={handleCancelRebalance}
                disabled={actionLoading}
                className="btn btn-danger"
                style={{ padding: '8px 20px', fontSize: '0.82rem' }}
              >
                <XCircle size={15} />
                <span>Cancel Rebalance</span>
              </button>
            ) : (
              <div
                style={{ display: 'inline-flex', alignItems: 'center' }}
                title={
                  !currentPlan
                    ? 'Generate plan preview first'
                    : currentPlan.total_migrations === 0
                    ? 'No migrations required'
                    : 'Execute rebalance plan'
                }
              >
                <button
                  id="btn-start-rebalance"
                  onClick={handleStartRebalance}
                  disabled={actionLoading || !currentPlan || currentPlan.total_migrations === 0}
                  className="btn btn-primary"
                  style={{
                    padding: '8px 20px',
                    fontSize: '0.82rem',
                    opacity: (!currentPlan || currentPlan.total_migrations === 0) ? 0.6 : 1,
                    cursor: (!currentPlan || currentPlan.total_migrations === 0) ? 'not-allowed' : 'pointer',
                  }}
                >
                  <Play size={15} />
                  <span>Start Rebalance</span>
                </button>
              </div>
            )}
          </div>
        </div>
      </motion.div>

      {/* Notifications */}
      <AnimatePresence>
        {error && (
          <motion.div
            initial={{ opacity: 0, y: -6 }}
            animate={{ opacity: 1, y: 0 }}
            exit={{ opacity: 0, y: -6 }}
            className="panel"
            style={{
              padding: '14px 18px',
              background: '#FEF2F2',
              borderColor: '#FECACA',
              display: 'flex',
              alignItems: 'center',
              justifyContent: 'space-between',
              gap: '12px',
            }}
          >
            <div style={{ display: 'flex', alignItems: 'center', gap: '10px', color: '#B91C1C', fontSize: '0.86rem' }}>
              <AlertTriangle size={18} color="#EF4444" style={{ flexShrink: 0 }} />
              <span>{error}</span>
            </div>
            <button
              onClick={() => setError(null)}
              aria-label="Dismiss error notification"
              style={{ background: 'transparent', border: 'none', cursor: 'pointer', color: '#B91C1C' }}
            >
              <X size={16} />
            </button>
          </motion.div>
        )}

        {successMsg && (
          <motion.div
            initial={{ opacity: 0, y: -6 }}
            animate={{ opacity: 1, y: 0 }}
            exit={{ opacity: 0, y: -6 }}
            className="panel"
            style={{
              padding: '14px 18px',
              background: '#ECFDF5',
              borderColor: '#A7F3D0',
              display: 'flex',
              alignItems: 'center',
              justifyContent: 'space-between',
              gap: '12px',
            }}
          >
            <div style={{ display: 'flex', alignItems: 'center', gap: '10px', color: '#047857', fontSize: '0.86rem' }}>
              <CheckCircle2 size={18} color="#10B981" style={{ flexShrink: 0 }} />
              <span>{successMsg}</span>
            </div>
            <button
              onClick={() => setSuccessMsg(null)}
              aria-label="Dismiss success notification"
              style={{ background: 'transparent', border: 'none', cursor: 'pointer', color: '#047857' }}
            >
              <X size={16} />
            </button>
          </motion.div>
        )}
      </AnimatePresence>

      {/* 2. LAST REBALANCE HISTORICAL SUMMARY (Explicitly separated from Current Plan) */}
      {lastCompletedRun && !isRunning && (
        <motion.div
          initial={{ opacity: 0, y: -4 }}
          animate={{ opacity: 1, y: 0 }}
          className="panel"
          style={{
            padding: '14px 20px',
            background: '#F8FAFC',
            border: '1px solid #E2E8F0',
            borderRadius: 'var(--radius-md)',
            display: 'flex',
            alignItems: 'center',
            justifyContent: 'space-between',
            flexWrap: 'wrap',
            gap: '14px',
          }}
        >
          <div style={{ display: 'flex', alignItems: 'center', gap: '12px' }}>
            <div
              style={{
                width: '32px',
                height: '32px',
                borderRadius: 'var(--radius-xs)',
                background: '#EFF6FF',
                display: 'flex',
                alignItems: 'center',
                justifyContent: 'center',
                color: '#2563EB',
                border: '1px solid #DBEAFE',
              }}
            >
              <Clock size={16} />
            </div>
            <div>
              <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                <span style={{ fontSize: '0.82rem', fontWeight: 700, color: '#172033', textTransform: 'uppercase', letterSpacing: '0.04em' }}>
                  Last Rebalance
                </span>
                <span className={`status-pill ${getRebalanceBadge(lastCompletedRun.status).className}`} style={{ fontSize: '0.66rem', padding: '2px 8px' }}>
                  {lastCompletedRun.status}
                </span>
                {lastCompletedRun.run_id && (
                  <span style={{ fontSize: '0.72rem', fontFamily: 'var(--font-mono)', color: '#64748B' }}>
                    ({lastCompletedRun.run_id})
                  </span>
                )}
              </div>
              <div style={{ fontSize: '0.74rem', color: '#64748B', marginTop: '1px' }}>
                {lastCompletedRun.completed_at
                  ? `Completed at ${new Date(lastCompletedRun.completed_at).toLocaleTimeString()}`
                  : 'Historical execution run'}
              </div>
            </div>
          </div>

          <div style={{ display: 'flex', alignItems: 'center', gap: '24px', flexWrap: 'wrap' }}>
            <div>
              <div style={{ fontSize: '0.66rem', color: '#64748B', textTransform: 'uppercase', fontWeight: 600 }}>
                Completed
              </div>
              <div style={{ fontSize: '0.88rem', fontWeight: 700, fontFamily: 'var(--font-mono)', color: '#172033' }}>
                {lastCompletedRun.completed_migrations || 0} / {lastCompletedRun.total_migrations || 0}
              </div>
            </div>

            <div>
              <div style={{ fontSize: '0.66rem', color: '#64748B', textTransform: 'uppercase', fontWeight: 600 }}>
                Bytes Transferred
              </div>
              <div style={{ fontSize: '0.88rem', fontWeight: 700, fontFamily: 'var(--font-mono)', color: '#7C3AED' }}>
                {formatBytes(lastCompletedRun.bytes_transferred || 0)}
              </div>
            </div>

            <div>
              <div style={{ fontSize: '0.66rem', color: '#64748B', textTransform: 'uppercase', fontWeight: 600 }}>
                Failed
              </div>
              <div style={{ fontSize: '0.88rem', fontWeight: 700, fontFamily: 'var(--font-mono)', color: lastCompletedRun.failed_migrations > 0 ? '#EF4444' : '#10B981' }}>
                {lastCompletedRun.failed_migrations || 0}
              </div>
            </div>

            <div>
              <div style={{ fontSize: '0.66rem', color: '#64748B', textTransform: 'uppercase', fontWeight: 600 }}>
                Status
              </div>
              <div style={{ fontSize: '0.88rem', fontWeight: 700, fontFamily: 'var(--font-mono)', color: '#10B981' }}>
                {lastCompletedRun.status}
              </div>
            </div>
          </div>
        </motion.div>
      )}

      {/* 3. METRIC CARDS RESPONSIVE GRID */}
      <div>
        <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: '14px', flexWrap: 'wrap', gap: '8px' }}>
          <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
            <Activity size={18} color="#2563EB" />
            <h3 style={{ fontSize: '1rem', fontWeight: 700, color: '#172033', letterSpacing: '-0.01em' }}>
              {isRunning ? 'Active Rebalance Execution Metrics' : 'Current Plan Workload & Target Metrics'}
            </h3>
          </div>
          {!isRunning && currentPlan && (
            <span style={{ fontSize: '0.76rem', color: '#64748B' }}>
              {currentPlan.total_migrations === 0
                ? 'No migrations required (cluster is in optimal balance)'
                : `${currentPlan.total_migrations} replica migration(s) queued`}
            </span>
          )}
        </div>

        <div
          style={{
            display: 'grid',
            gridTemplateColumns: 'repeat(auto-fit, minmax(200px, 1fr))',
            gap: '16px',
            width: '100%',
          }}
        >
          {/* Card 1: Overall Progress */}
          <motion.div whileHover={{ y: -2 }} className="stat-card">
            <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start' }}>
              <div className="stat-icon-wrapper" style={{ background: '#EFF6FF', color: '#2563EB' }}>
                <Activity size={18} />
              </div>
              <span className={`status-pill ${isRunning ? 'recovering' : currentPlan?.total_migrations === 0 ? 'healthy' : 'warning'}`} style={{ fontSize: '0.65rem' }}>
                {isRunning ? 'RUNNING' : currentPlan?.total_migrations === 0 ? 'BALANCED' : 'READY'}
              </span>
            </div>
            <div style={{ fontSize: '1.75rem', fontWeight: 800, color: '#172033', fontFamily: 'var(--font-mono)' }}>
              {progressPercent}%
            </div>
            <div style={{ fontSize: '0.82rem', fontWeight: 600, color: '#172033' }}>
              Overall Progress
            </div>
            <div className="progress-container" style={{ marginTop: '4px', height: '6px' }}>
              <div
                className="progress-fill"
                style={{
                  width: `${progressPercent}%`,
                  transition: isRunning ? 'width 0.5s ease' : 'none',
                }}
              />
            </div>
          </motion.div>

          {/* Card 2: Migrations Done/Total */}
          <motion.div whileHover={{ y: -2 }} className="stat-card">
            <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start' }}>
              <div className="stat-icon-wrapper" style={{ background: '#ECFDF5', color: '#10B981' }}>
                <Layers size={18} />
              </div>
              <span className={`status-pill ${isRunning ? 'recovering' : 'healthy'}`} style={{ fontSize: '0.65rem' }}>
                {isRunning ? 'RUNNING' : currentPlan?.total_migrations === 0 ? 'OPTIMAL' : 'QUEUE'}
              </span>
            </div>
            <div style={{ fontSize: '1.75rem', fontWeight: 800, color: '#172033', fontFamily: 'var(--font-mono)' }}>
              {completedMigrations} <span style={{ fontSize: '1.1rem', color: '#94A3B8' }}>/ {totalMigrations}</span>
            </div>
            <div style={{ fontSize: '0.82rem', fontWeight: 600, color: '#172033' }}>
              Migrations
            </div>
            <div style={{ fontSize: '0.72rem', color: '#64748B' }}>
              {isRunning
                ? `${completedMigrations} completed of ${totalMigrations} total`
                : `${totalMigrations} migration(s) scheduled in current plan`}
            </div>
          </motion.div>

          {/* Card 3: Failed */}
          <motion.div whileHover={{ y: -2 }} className="stat-card">
            <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start' }}>
              <div className="stat-icon-wrapper" style={{ background: failedMigrations > 0 ? '#FEF2F2' : '#F8FAFC', color: failedMigrations > 0 ? '#EF4444' : '#64748B' }}>
                <AlertTriangle size={18} />
              </div>
              <span className={`status-pill ${failedMigrations > 0 ? 'failure' : 'healthy'}`} style={{ fontSize: '0.65rem' }}>
                {failedMigrations > 0 ? 'FAILURES' : 'ZERO ERRORS'}
              </span>
            </div>
            <div style={{ fontSize: '1.75rem', fontWeight: 800, color: failedMigrations > 0 ? '#EF4444' : '#172033', fontFamily: 'var(--font-mono)' }}>
              {failedMigrations}
            </div>
            <div style={{ fontSize: '0.82rem', fontWeight: 600, color: '#172033' }}>
              Failed
            </div>
            <div style={{ fontSize: '0.72rem', color: '#64748B' }}>
              {failedMigrations > 0 ? `${failedMigrations} replica migrations failed` : 'Zero errors encountered'}
            </div>
          </motion.div>

          {/* Card 4: Bytes Transferred */}
          <motion.div whileHover={{ y: -2 }} className="stat-card">
            <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start' }}>
              <div className="stat-icon-wrapper" style={{ background: '#F5F3FF', color: '#7C3AED' }}>
                <Database size={18} />
              </div>
              <span className={`status-pill ${isRunning ? 'recovering' : 'healthy'}`} style={{ fontSize: '0.65rem' }}>
                {isRunning ? 'TRANSFERRED' : 'IDLE'}
              </span>
            </div>
            <div style={{ fontSize: '1.75rem', fontWeight: 800, color: '#7C3AED', fontFamily: 'var(--font-mono)' }}>
              {formatBytes(bytesTransferred)}
            </div>
            <div style={{ fontSize: '0.82rem', fontWeight: 600, color: '#172033' }}>
              Bytes Transferred
            </div>
            <div style={{ fontSize: '0.72rem', color: '#64748B' }}>
              {isRunning ? 'Physical bytes written & verified' : '0 B transferred in current plan'}
            </div>
          </motion.div>

          {/* Card 5: Total to Move */}
          <motion.div whileHover={{ y: -2 }} className="stat-card">
            <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start' }}>
              <div className="stat-icon-wrapper" style={{ background: '#ECFEFF', color: '#06B6D4' }}>
                <HardDrive size={18} />
              </div>
              <span className="status-pill healthy" style={{ fontSize: '0.65rem' }}>
                ESTIMATE
              </span>
            </div>
            <div style={{ fontSize: '1.75rem', fontWeight: 800, color: '#172033', fontFamily: 'var(--font-mono)' }}>
              {formatBytes(bytesTotal)}
            </div>
            <div style={{ fontSize: '0.82rem', fontWeight: 600, color: '#172033' }}>
              Total to Move
            </div>
            <div style={{ fontSize: '0.72rem', color: '#64748B' }}>
              Planned payload volume
            </div>
          </motion.div>

          {/* Card 6: Affected Objects */}
          <motion.div whileHover={{ y: -2 }} className="stat-card">
            <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start' }}>
              <div className="stat-icon-wrapper" style={{ background: '#F0FDFA', color: '#0D9488' }}>
                <ShieldCheck size={18} />
              </div>
              <span className="status-pill healthy" style={{ fontSize: '0.65rem' }}>
                DELTA
              </span>
            </div>
            <div style={{ fontSize: '1.75rem', fontWeight: 800, color: '#172033', fontFamily: 'var(--font-mono)' }}>
              {affectedObjects}
            </div>
            <div style={{ fontSize: '0.82rem', fontWeight: 600, color: '#172033' }}>
              Affected Objects
            </div>
            <div style={{ fontSize: '0.72rem', color: '#64748B' }}>
              Objects with placement shifts
            </div>
          </motion.div>

          {/* Card 7: Catalog Size */}
          <motion.div whileHover={{ y: -2 }} className="stat-card">
            <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start' }}>
              <div className="stat-icon-wrapper" style={{ background: '#EFF6FF', color: '#2563EB' }}>
                <Server size={18} />
              </div>
              <span className="status-pill healthy" style={{ fontSize: '0.65rem' }}>
                CATALOG
              </span>
            </div>
            <div style={{ fontSize: '1.75rem', fontWeight: 800, color: '#172033', fontFamily: 'var(--font-mono)' }}>
              {totalObjects}
            </div>
            <div style={{ fontSize: '0.82rem', fontWeight: 600, color: '#172033' }}>
              Catalog Size
            </div>
            <div style={{ fontSize: '0.72rem', color: '#64748B' }}>
              Total objects stored in cluster
            </div>
          </motion.div>
        </div>
      </div>

      {/* 3. CLUSTER MEMBERSHIP TOPOLOGY */}
      <div>
        <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: '14px', flexWrap: 'wrap', gap: '8px' }}>
          <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
            <Server size={18} color="#2563EB" />
            <h3 style={{ fontSize: '1rem', fontWeight: 700, color: '#172033', letterSpacing: '-0.01em' }}>
              Cluster Membership Topology ({nodes.length} Nodes)
            </h3>
          </div>
          <span style={{ fontSize: '0.75rem', color: '#64748B', fontFamily: 'var(--font-mono)' }}>
            {healthyNodesCount}/{nodes.length} OPERATIONAL &amp; ELIGIBLE
          </span>
        </div>

        <div
          style={{
            display: 'grid',
            gridTemplateColumns: 'repeat(auto-fit, minmax(230px, 1fr))',
            gap: '16px',
            width: '100%',
          }}
        >
          {nodes.map((node) => {
            const rawStatus = (node.status || '').toUpperCase();
            const badge = getNodeStatusBadge(rawStatus);
            const isHealthy = rawStatus === 'HEALTHY';
            const isDecommissioning = rawStatus === 'DECOMMISSIONING';
            const isDecom = rawStatus === 'DECOMMISSIONED';
            const isEligible = isHealthy;

            return (
              <motion.div
                key={node.node_id}
                whileHover={{ y: -2 }}
                className="node-card"
                style={{
                  padding: '20px',
                  background: isDecom ? '#F8FAFC' : '#FFFFFF',
                  opacity: isDecom ? 0.75 : 1,
                  borderLeft: `4px solid ${badge.border}`,
                }}
              >
                <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
                  <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                    <HardDrive size={18} color={isHealthy ? '#10B981' : isDecom ? '#94A3B8' : '#F59E0B'} />
                    <span style={{ fontSize: '1rem', fontWeight: 800, color: '#172033', fontFamily: 'var(--font-mono)' }}>
                      {node.node_id}
                    </span>
                  </div>
                  <span className={`status-pill ${badge.className}`} style={{ fontSize: '0.68rem' }}>
                    <span className="pulse-dot" />
                    {badge.label}
                  </span>
                </div>

                <div className="tech-inset" style={{ padding: '10px 12px', fontSize: '0.76rem' }}>
                  <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
                    <span style={{ color: '#64748B' }}>ENDPOINT</span>
                    <span style={{ color: '#172033', fontWeight: 600, fontFamily: 'var(--font-mono)' }}>
                      {node.host || 'localhost'}:{node.port}
                    </span>
                  </div>
                  <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginTop: '6px' }}>
                    <span style={{ color: '#64748B' }}>OBJECTS</span>
                    <span style={{ color: '#172033', fontWeight: 700, fontFamily: 'var(--font-mono)' }}>
                      {node.objects_count || 0} replicas
                    </span>
                  </div>
                </div>

                <div
                  style={{
                    display: 'flex',
                    justifyContent: 'space-between',
                    alignItems: 'center',
                    fontSize: '0.74rem',
                    borderTop: '1px solid #F1F5F9',
                    paddingTop: '10px',
                  }}
                >
                  <span style={{ color: '#64748B' }}>PLACEMENT:</span>
                  <span
                    style={{
                      fontWeight: 700,
                      fontFamily: 'var(--font-mono)',
                      color: isEligible ? '#10B981' : isDecommissioning ? '#F59E0B' : '#64748B',
                    }}
                  >
                    {isEligible ? 'ELIGIBLE (HRW)' : isDecommissioning ? 'DRAINING' : isDecom ? 'INELIGIBLE' : 'SUSPENDED'}
                  </span>
                </div>
              </motion.div>
            );
          })}
        </div>
      </div>

      {/* 4. REBALANCE PLAN SECTION */}
      <div className="panel" style={{ overflow: 'hidden' }}>
        <div
          className="panel-header"
          style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', flexWrap: 'wrap', gap: '12px' }}
        >
          <div style={{ display: 'flex', alignItems: 'center', gap: '10px' }}>
            <Layers size={18} color="#2563EB" />
            <div>
              <h3 style={{ fontSize: '1rem', fontWeight: 700, color: '#172033' }}>
                Dry-Run Migration Plan
              </h3>
              <span style={{ fontSize: '0.75rem', color: '#64748B' }}>
                {currentPlan?.migrations?.length || 0} scheduled replica relocation operations
              </span>
            </div>
          </div>

          {!currentPlan ? (
            <span className="status-pill warning">
              <Clock size={13} />
              PLAN NOT GENERATED
            </span>
          ) : currentPlan.total_migrations === 0 ? (
            <span className="status-pill healthy">
              <CheckCircle2 size={13} />
              NO MIGRATIONS REQUIRED (0 CHURN)
            </span>
          ) : (
            <span className="status-pill warning">
              <Clock size={13} />
              {currentPlan.total_migrations} MIGRATIONS PENDING
            </span>
          )}
        </div>

        {/* Plan Table with Horizontal Scrolling */}
        <div style={{ overflowX: 'auto', width: '100%' }}>
          {!currentPlan ? (
            <div style={{ padding: '48px 24px', textAlign: 'center' }}>
              <Layers size={40} color="#94A3B8" style={{ margin: '0 auto 12px' }} />
              <h4 style={{ fontSize: '1.05rem', fontWeight: 700, color: '#172033' }}>
                No Plan Generated
              </h4>
              <p style={{ fontSize: '0.84rem', color: '#64748B', marginTop: '6px', maxWidth: '520px', margin: '6px auto 0' }}>
                Click "Preview Plan" above to calculate Rendezvous Hashing (HRW) migrations across the cluster.
              </p>
            </div>
          ) : !currentPlan.migrations || currentPlan.migrations.length === 0 ? (
            <div style={{ padding: '48px 24px', textAlign: 'center' }}>
              <CheckCircle2 size={40} color="#10B981" style={{ margin: '0 auto 12px' }} />
              <h4 style={{ fontSize: '1.05rem', fontWeight: 700, color: '#172033' }}>
                No Migrations Required
              </h4>
              <p style={{ fontSize: '0.84rem', color: '#64748B', marginTop: '6px', maxWidth: '520px', margin: '6px auto 0' }}>
                Current replica placement across healthy nodes matches the calculated Rendezvous Hashing (HRW) distribution. Zero data migrations required.
              </p>
            </div>
          ) : (
            <table style={{ width: '100%', borderCollapse: 'collapse', textAlign: 'left', fontSize: '0.84rem' }}>
              <thead>
                <tr style={{ background: '#F8FAFC', borderBottom: '1px solid #E2E8F0' }}>
                  <th style={{ padding: '12px 18px', color: '#64748B', fontWeight: 600, fontSize: '0.74rem', textTransform: 'uppercase', letterSpacing: '0.05em' }}>
                    Object ID
                  </th>
                  <th style={{ padding: '12px 18px', color: '#64748B', fontWeight: 600, fontSize: '0.74rem', textTransform: 'uppercase', letterSpacing: '0.05em' }}>
                    Source Node
                  </th>
                  <th style={{ padding: '12px 18px', color: '#64748B', fontWeight: 600, fontSize: '0.74rem', textTransform: 'uppercase', letterSpacing: '0.05em' }}>
                    Target Node
                  </th>
                  <th style={{ padding: '12px 18px', color: '#64748B', fontWeight: 600, fontSize: '0.74rem', textTransform: 'uppercase', letterSpacing: '0.05em' }}>
                    Bytes
                  </th>
                  <th style={{ padding: '12px 18px', color: '#64748B', fontWeight: 600, fontSize: '0.74rem', textTransform: 'uppercase', letterSpacing: '0.05em' }}>
                    SHA-256
                  </th>
                  <th style={{ padding: '12px 18px', color: '#64748B', fontWeight: 600, fontSize: '0.74rem', textTransform: 'uppercase', letterSpacing: '0.05em' }}>
                    Reason
                  </th>
                  <th style={{ padding: '12px 18px', color: '#64748B', fontWeight: 600, fontSize: '0.74rem', textTransform: 'uppercase', letterSpacing: '0.05em' }}>
                    Status
                  </th>
                </tr>
              </thead>
              <tbody>
                {currentPlan.migrations.map((m, idx) => {
                  const reasonBadge = getReasonBadge(m.reason);
                  const isMigCompleted = m.status === 'COMPLETED';
                  const isMigFailed = m.status === 'FAILED';
                  const isMigRunning = m.status === 'RUNNING' || m.status === 'MIGRATING';

                  return (
                    <tr
                      key={`${m.object_id}-${m.source_node}-${m.target_node}-${idx}`}
                      style={{
                        borderBottom: '1px solid #F1F5F9',
                        transition: 'background 0.15s ease',
                      }}
                      onMouseEnter={(e) => (e.currentTarget.style.background = '#F8FAFC')}
                      onMouseLeave={(e) => (e.currentTarget.style.background = 'transparent')}
                    >
                      <td style={{ padding: '14px 18px', fontFamily: 'var(--font-mono)', fontWeight: 600, color: '#172033' }}>
                        <span title={m.object_id}>
                          {m.object_id.length > 20 ? `${m.object_id.slice(0, 16)}...` : m.object_id}
                        </span>
                      </td>
                      <td style={{ padding: '14px 18px' }}>
                        <span className="status-pill warning" style={{ fontSize: '0.72rem' }}>
                          {m.source_node}
                        </span>
                      </td>
                      <td style={{ padding: '14px 18px' }}>
                        <div style={{ display: 'flex', alignItems: 'center', gap: '6px' }}>
                          <ArrowRight size={13} color="#64748B" />
                          <span className="status-pill healthy" style={{ fontSize: '0.72rem' }}>
                            {m.target_node}
                          </span>
                        </div>
                      </td>
                      <td style={{ padding: '14px 18px', fontFamily: 'var(--font-mono)', color: '#475569' }}>
                        {formatBytes(m.bytes)}
                      </td>
                      <td style={{ padding: '14px 18px', fontFamily: 'var(--font-mono)', color: '#64748B', fontSize: '0.74rem' }}>
                        <span title={m.sha256}>
                          {m.sha256 ? `${m.sha256.slice(0, 12)}...` : '—'}
                        </span>
                      </td>
                      <td style={{ padding: '14px 18px' }}>
                        <span
                          style={{
                            fontSize: '0.70rem',
                            fontFamily: 'var(--font-mono)',
                            padding: '3px 8px',
                            borderRadius: '4px',
                            fontWeight: 700,
                            background: reasonBadge.bg,
                            color: reasonBadge.color,
                            border: `1px solid ${reasonBadge.border}`,
                          }}
                        >
                          {reasonBadge.label}
                        </span>
                      </td>
                      <td style={{ padding: '14px 18px' }}>
                        <span
                          className={`status-pill ${
                            isMigCompleted ? 'healthy' : isMigFailed ? 'failure' : isMigRunning ? 'recovering' : 'warning'
                          }`}
                          style={{ fontSize: '0.68rem' }}
                        >
                          {m.status || 'PENDING'}
                        </span>
                      </td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          )}
        </div>
      </div>

      {/* 5. REGISTER NODE MODAL */}
      <AnimatePresence>
        {showRegisterModal && (
          <div
            style={{
              position: 'fixed',
              top: 0,
              left: 0,
              right: 0,
              bottom: 0,
              background: 'rgba(15, 23, 42, 0.55)',
              backdropFilter: 'blur(4px)',
              zIndex: 9999,
              display: 'flex',
              alignItems: 'center',
              justifyContent: 'center',
              padding: '20px',
            }}
          >
            <motion.div
              initial={{ opacity: 0, scale: 0.95 }}
              animate={{ opacity: 1, scale: 1 }}
              exit={{ opacity: 0, scale: 0.95 }}
              className="panel"
              style={{
                width: '100%',
                maxWidth: '500px',
                borderRadius: 'var(--radius-lg)',
                boxShadow: 'var(--shadow-xl)',
                overflow: 'hidden',
              }}
            >
              <div className="panel-header" style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
                <div style={{ display: 'flex', alignItems: 'center', gap: '10px' }}>
                  <PlusCircle size={20} color="#2563EB" />
                  <h3 style={{ fontSize: '1.05rem', fontWeight: 700, color: '#172033' }}>
                    Register Storage Node
                  </h3>
                </div>
                <button
                  onClick={() => setShowRegisterModal(false)}
                  aria-label="Close register node dialog"
                  style={{ background: 'transparent', border: 'none', cursor: 'pointer', color: '#64748B' }}
                >
                  <X size={18} />
                </button>
              </div>

              <form onSubmit={handleRegisterNode} style={{ padding: '24px', display: 'flex', flexDirection: 'column', gap: '16px' }}>
                <p style={{ fontSize: '0.84rem', color: '#64748B' }}>
                  Register a newly deployed storage daemon into the cluster health monitoring loop and HRW placement eligibility.
                </p>

                <div>
                  <label htmlFor="reg-node-id" style={{ display: 'block', fontSize: '0.78rem', fontWeight: 700, color: '#172033', marginBottom: '6px' }}>
                    NODE IDENTIFIER (e.g. node-6)
                  </label>
                  <input
                    id="reg-node-id"
                    type="text"
                    required
                    placeholder="node-6"
                    value={newNodeId}
                    onChange={(e) => setNewNodeId(e.target.value)}
                    style={{
                      width: '100%',
                      padding: '9px 12px',
                      border: '1px solid #CBD5E1',
                      borderRadius: 'var(--radius-sm)',
                      fontSize: '0.85rem',
                      fontFamily: 'var(--font-mono)',
                      outline: 'none',
                    }}
                  />
                </div>

                <div style={{ display: 'grid', gridTemplateColumns: '2fr 1fr', gap: '12px' }}>
                  <div>
                    <label htmlFor="reg-node-host" style={{ display: 'block', fontSize: '0.78rem', fontWeight: 700, color: '#172033', marginBottom: '6px' }}>
                      HOST / CONTAINER
                    </label>
                    <input
                      id="reg-node-host"
                      type="text"
                      required
                      placeholder="localhost"
                      value={newHost}
                      onChange={(e) => setNewHost(e.target.value)}
                      style={{
                        width: '100%',
                        padding: '9px 12px',
                        border: '1px solid #CBD5E1',
                        borderRadius: 'var(--radius-sm)',
                        fontSize: '0.85rem',
                        fontFamily: 'var(--font-mono)',
                        outline: 'none',
                      }}
                    />
                  </div>

                  <div>
                    <label htmlFor="reg-node-port" style={{ display: 'block', fontSize: '0.78rem', fontWeight: 700, color: '#172033', marginBottom: '6px' }}>
                      PORT
                    </label>
                    <input
                      id="reg-node-port"
                      type="number"
                      required
                      placeholder="8006"
                      value={newPort}
                      onChange={(e) => setNewPort(e.target.value)}
                      style={{
                        width: '100%',
                        padding: '9px 12px',
                        border: '1px solid #CBD5E1',
                        borderRadius: 'var(--radius-sm)',
                        fontSize: '0.85rem',
                        fontFamily: 'var(--font-mono)',
                        outline: 'none',
                      }}
                    />
                  </div>
                </div>

                <div>
                  <label htmlFor="reg-node-url" style={{ display: 'block', fontSize: '0.78rem', fontWeight: 700, color: '#172033', marginBottom: '6px' }}>
                    CUSTOM URL (OPTIONAL)
                  </label>
                  <input
                    id="reg-node-url"
                    type="text"
                    placeholder="http://vault-node-6:8000"
                    value={newUrl}
                    onChange={(e) => setNewUrl(e.target.value)}
                    style={{
                      width: '100%',
                      padding: '9px 12px',
                      border: '1px solid #CBD5E1',
                      borderRadius: 'var(--radius-sm)',
                      fontSize: '0.85rem',
                      fontFamily: 'var(--font-mono)',
                      outline: 'none',
                    }}
                  />
                </div>

                <div style={{ display: 'flex', justifyContent: 'flex-end', gap: '10px', marginTop: '10px' }}>
                  <button
                    type="button"
                    onClick={() => setShowRegisterModal(false)}
                    className="btn"
                  >
                    Cancel
                  </button>
                  <button
                    type="submit"
                    disabled={actionLoading}
                    className="btn btn-primary"
                  >
                    {actionLoading ? 'Registering...' : 'Register Node'}
                  </button>
                </div>
              </form>
            </motion.div>
          </div>
        )}
      </AnimatePresence>

      {/* 6. DECOMMISSION NODE MODAL */}
      <AnimatePresence>
        {showDecommissionModal && (
          <div
            style={{
              position: 'fixed',
              top: 0,
              left: 0,
              right: 0,
              bottom: 0,
              background: 'rgba(15, 23, 42, 0.55)',
              backdropFilter: 'blur(4px)',
              zIndex: 9999,
              display: 'flex',
              alignItems: 'center',
              justifyContent: 'center',
              padding: '20px',
            }}
          >
            <motion.div
              initial={{ opacity: 0, scale: 0.95 }}
              animate={{ opacity: 1, scale: 1 }}
              exit={{ opacity: 0, scale: 0.95 }}
              className="panel"
              style={{
                width: '100%',
                maxWidth: '500px',
                borderRadius: 'var(--radius-lg)',
                boxShadow: 'var(--shadow-xl)',
                overflow: 'hidden',
              }}
            >
              <div className="panel-header" style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
                <div style={{ display: 'flex', alignItems: 'center', gap: '10px' }}>
                  <Trash2 size={20} color="#EF4444" />
                  <h3 style={{ fontSize: '1.05rem', fontWeight: 700, color: '#172033' }}>
                    Decommission Storage Node
                  </h3>
                </div>
                <button
                  onClick={() => setShowDecommissionModal(false)}
                  aria-label="Close decommission node dialog"
                  style={{ background: 'transparent', border: 'none', cursor: 'pointer', color: '#64748B' }}
                >
                  <X size={18} />
                </button>
              </div>

              <form onSubmit={handleDecommissionNode} style={{ padding: '24px', display: 'flex', flexDirection: 'column', gap: '16px' }}>
                <div
                  style={{
                    background: '#FFFBEB',
                    border: '1px solid #FDE68A',
                    borderRadius: 'var(--radius-sm)',
                    padding: '12px 14px',
                    fontSize: '0.82rem',
                    color: '#92400E',
                    lineHeight: 1.5,
                  }}
                >
                  <strong>Copy-Before-Delete Invariant:</strong> Decommissioning will safely drain all replicas from the selected node to healthy peer nodes before retiring it. At no point will RF=3 availability be reduced.
                </div>

                <div>
                  <label htmlFor="decom-node-select" style={{ display: 'block', fontSize: '0.78rem', fontWeight: 700, color: '#172033', marginBottom: '6px' }}>
                    SELECT NODE TO DECOMMISSION
                  </label>
                  <select
                    id="decom-node-select"
                    required
                    value={decomNodeId}
                    onChange={(e) => setDecomNodeId(e.target.value)}
                    style={{
                      width: '100%',
                      padding: '9px 12px',
                      border: '1px solid #CBD5E1',
                      borderRadius: 'var(--radius-sm)',
                      fontSize: '0.85rem',
                      fontFamily: 'var(--font-mono)',
                      outline: 'none',
                      background: '#FFFFFF',
                    }}
                  >
                    <option value="">-- Choose Storage Node --</option>
                    {nodes
                      .filter((n) => (n.status || '').toUpperCase() !== 'DECOMMISSIONED')
                      .map((n) => (
                        <option key={n.node_id} value={n.node_id}>
                          {n.node_id} ({n.status || 'UNKNOWN'}) — {n.objects_count || 0} replicas
                        </option>
                      ))}
                  </select>
                </div>

                <div style={{ display: 'flex', justifyContent: 'flex-end', gap: '10px', marginTop: '10px' }}>
                  <button
                    type="button"
                    onClick={() => setShowDecommissionModal(false)}
                    className="btn"
                  >
                    Cancel
                  </button>
                  <button
                    type="submit"
                    disabled={actionLoading || !decomNodeId}
                    className="btn btn-danger"
                  >
                    {actionLoading ? 'Draining...' : 'Drain & Decommission'}
                  </button>
                </div>
              </form>
            </motion.div>
          </div>
        )}
      </AnimatePresence>
    </div>
  );
}
