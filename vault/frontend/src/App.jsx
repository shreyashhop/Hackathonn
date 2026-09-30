import React, { useState, useEffect, useRef, useCallback } from 'react';
import Sidebar from './components/Sidebar';
import Header from './components/Header';
import DashboardView from './views/DashboardView';
import NodesView from './views/NodesView';
import EventsView from './views/EventsView';
import ObjectsView from './views/ObjectsView';
import ReplicationView from './views/ReplicationView';
import RepairView from './views/RepairView';
import FaultLabView from './views/FaultLabView';
import IntegrityView from './views/IntegrityView';
import RebalanceView from './views/RebalanceView';
import PlaceholderView from './views/PlaceholderView';
// Coordinator URL resolution:
// In dev with Vite proxy: relative path works.
// Directly hitting coordinator port 8000 as fallback.
const COORDINATOR_BASE_URL = '';
const WS_BASE_URL = COORDINATOR_BASE_URL
  ? COORDINATOR_BASE_URL.replace('http://', 'ws://').replace('https://', 'wss://')
  : `${window.location.protocol === 'https:' ? 'wss:' : 'ws:'}//${window.location.host}`;
export default function App() {
  const [currentTab, setCurrentTab] = useState('dashboard');
  const [nodes, setNodes] = useState([]);
  const [coordinatorData, setCoordinatorData] = useState(null);
  const [wsConnected, setWsConnected] = useState(false);
  const [events, setEvents] = useState([]);
  const [refreshing, setRefreshing] = useState(false);
  const [error, setError] = useState(null);
  const [lastUpdated, setLastUpdated] = useState(null);
  const [liveAnnouncement, setLiveAnnouncement] = useState('');
  const wsRef = useRef(null);
  const reconnectTimeoutRef = useRef(null);
  // Fetch node statuses from coordinator
  const fetchNodes = useCallback(async () => {
    try {
      setRefreshing(true);
      // Try direct coordinator endpoint first, then proxy fallback
      let res;
      try {
        res = await fetch(`${COORDINATOR_BASE_URL}/nodes`);
      } catch {
        res = await fetch('/nodes');
      }
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
      const data = await res.json();
      setNodes(data);
      setError(null);
      setLastUpdated(new Date());
    } catch (err) {
      console.warn('Failed to fetch /nodes:', err.message);
      setError(err.message);
    } finally {
      setRefreshing(false);
    }
  }, []);
  // Fetch coordinator health
  const fetchCoordinatorHealth = useCallback(async () => {
    try {
      let res;
      try {
        res = await fetch(`${COORDINATOR_BASE_URL}/health`);
      } catch {
        res = await fetch('/health');
      }
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
      const data = await res.json();
      setCoordinatorData(data);
      if (data.nodes && (!nodes || nodes.length === 0)) {
        setNodes(data.nodes);
      }
    } catch (err) {
      setCoordinatorData(null);
    }
  }, [nodes]);
  const refreshAll = useCallback(async () => {
    await Promise.all([fetchNodes(), fetchCoordinatorHealth()]);
  }, [fetchNodes, fetchCoordinatorHealth]);
  // Setup WebSocket connection
  useEffect(() => {
    let isSubscribed = true;
    function connectWebSocket() {
      const targetWsUrl = `${WS_BASE_URL}/events/ws`;
      console.log('[WebSocket] Connecting to:', targetWsUrl);
      try {
        const ws = new WebSocket(targetWsUrl);
        wsRef.current = ws;
        ws.onopen = () => {
          if (!isSubscribed) return;
          console.log('[WebSocket] Connection established');
          setWsConnected(true);
        };
        ws.onmessage = (event) => {
          if (!isSubscribed) return;
          try {
            const data = JSON.parse(event.data);
            setEvents((prev) => [data, ...prev.slice(0, 49)]);
            // Accessible screen reader announcement for significant milestones
            if (data.event_type === 'NODE_DOWN') {
              setLiveAnnouncement(`Storage node ${data.node_id} is down.`);
            } else if (data.event_type === 'NODE_HEALTHY' || data.event_type === 'NODE_RECOVERED') {
              setLiveAnnouncement(`Storage node ${data.node_id} is now healthy.`);
            } else if (data.event_type === 'NODE_PARTITIONED') {
              setLiveAnnouncement(`Storage node ${data.node_id} has been network partitioned.`);
            } else if (data.event_type === 'REPAIR_COMPLETED' || data.event_type === 'RECONCILIATION_COMPLETED') {
              setLiveAnnouncement(`Replica repair completed for object ${data.object_name || data.object_id || ''}.`);
            } else if (data.event_type === 'INTEGRITY_SCAN_COMPLETED') {
              setLiveAnnouncement('Data integrity audit scan completed.');
            } else if (data.event_type === 'OBJECT_STORED') {
              setLiveAnnouncement(`Object ${data.object_name || data.object_id || ''} stored successfully.`);
            }
            // Trigger node refresh if cluster state or object storage changed
            if ([
              'NODE_STATUS_CHANGED',
              'NODE_STATE_CHANGED',
              'NODE_SUSPECT',
              'NODE_DOWN',
              'NODE_RECOVERING',
              'NODE_RECOVERED',
              'REPAIR_QUEUED',
              'REPAIR_STARTED',
              'REPAIR_COMPLETED',
              'REPAIR_FAILED',
              'REPLICA_AT_RISK',
              'REPLICA_CORRUPT',
              'INTEGRITY_SCAN_STARTED',
              'INTEGRITY_SCAN_COMPLETED',
              'INTEGRITY_CHECK_STARTED',
              'INTEGRITY_CHECK_PASSED',
              'INTEGRITY_CHECK_FAILED',
              'OBJECT_STORED',
              'OBJECT_DELETED',
              'REPLICATION_COMPLETED',
              'REPLICA_STORED',
              'REPLICA_DELETE',
              'WRITE_QUORUM_SATISFIED'
            ].includes(data.event_type)) {
              refreshAll();
            }
          } catch (e) {
            console.error('WebSocket parse error:', e);
          }
        };
        ws.onclose = () => {
          if (!isSubscribed) return;
          setWsConnected(false);
          console.log('[WebSocket] Connection closed. Retrying in 3s...');
          reconnectTimeoutRef.current = setTimeout(connectWebSocket, 3000);
        };
        ws.onerror = (err) => {
          console.warn('[WebSocket] Error occurred');
          ws.close();
        };
      } catch (err) {
        console.error('[WebSocket] Setup error:', err);
        reconnectTimeoutRef.current = setTimeout(connectWebSocket, 3000);
      }
    }
    connectWebSocket();
    return () => {
      isSubscribed = false;
      if (reconnectTimeoutRef.current) clearTimeout(reconnectTimeoutRef.current);
      if (wsRef.current) wsRef.current.close();
    };
  }, [fetchNodes]);
  // Initial data poll and periodic refresh every 6 seconds
  useEffect(() => {
    refreshAll();
    const interval = setInterval(refreshAll, 6000);
    return () => clearInterval(interval);
  }, [refreshAll]);
  const healthyNodesCount = nodes.filter((n) => (n.status || '').toLowerCase() === 'healthy').length;
  const getPageTitle = () => {
    switch (currentTab) {
      case 'dashboard':
        return 'System Overview';
      case 'nodes':
        return 'Storage Nodes';
      case 'objects':
        return 'Object Catalog';
      case 'replication':
        return 'Replication Mesh';
      case 'repair':
      case 'repairs':
        return 'Repair Center';
      case 'integrity':
        return 'Data Integrity Auditor';
      case 'fault_lab':
        return 'Fault Injection Simulator';
      case 'events':
        return 'Live Events';
      default:
        return currentTab.charAt(0).toUpperCase() + currentTab.slice(1).replace('_', ' ');
    }
  };
  return (
    <div className="app-container">
      {/* Skip to Main Content Link for Keyboard Navigation */}
      <a href="#main-content" className="skip-link">
        Skip to main content
      </a>
      {/* Screen Reader Live Region for Milestones */}
      <div className="sr-only" role="status" aria-live="polite" aria-atomic="true">
        {liveAnnouncement}
      </div>
      <Sidebar
        currentTab={currentTab}
        onSelectTab={setCurrentTab}
        nodeCount={nodes.length || 5}
        healthyNodeCount={healthyNodesCount}
      />
      <div className="main-content">
        <Header
          title={getPageTitle()}
          wsConnected={wsConnected}
          coordinatorHealthy={!!coordinatorData}
          healthyCount={healthyNodesCount}
          totalNodes={nodes.length || 5}
          onRefresh={refreshAll}
          refreshing={refreshing}
          lastUpdated={lastUpdated}
        />
        <main id="main-content" className="content-scrollable">
          {currentTab === 'dashboard' && (
            <DashboardView
              nodes={nodes}
              coordinatorData={coordinatorData}
              recentEvents={events}
              loading={refreshing}
              error={error}
              wsConnected={wsConnected}
            />
          )}
          {currentTab === 'nodes' && (
            <NodesView
              nodes={nodes}
              onRefresh={refreshAll}
              refreshing={refreshing}
            />
          )}
          {currentTab === 'objects' && (
            <ObjectsView
              coordinatorBaseUrl={COORDINATOR_BASE_URL}
              onRefreshNodes={refreshAll}
            />
          )}
          {currentTab === 'replication' && (
            <ReplicationView
              coordinatorBaseUrl={COORDINATOR_BASE_URL}
              nodes={nodes}
            />
          )}
          {(currentTab === 'repair' || currentTab === 'repairs') && (
            <RepairView
              coordinatorBaseUrl={COORDINATOR_BASE_URL}
            />
          )}
          {currentTab === 'integrity' && (
            <IntegrityView
              coordinatorBaseUrl={COORDINATOR_BASE_URL}
            />
          )}
          {currentTab === 'fault_lab' && (
            <FaultLabView
              coordinatorBaseUrl={COORDINATOR_BASE_URL}
              onRefreshCluster={refreshAll}
            />
          )}
          {currentTab === 'events' && (
            <EventsView
              events={events}
              onClear={() => setEvents([])}
              wsConnected={wsConnected}
            />
          )}
          {(currentTab === 'rebalancing' || currentTab === 'rebalance') && (
            <RebalanceView
              coordinatorBaseUrl={COORDINATOR_BASE_URL}
              nodes={nodes}
              onRefreshNodes={refreshAll}
            />
          )}
          {!['dashboard', 'nodes', 'events', 'objects', 'replication', 'repair', 'repairs', 'integrity', 'fault_lab', 'rebalancing', 'rebalance'].includes(currentTab) && (
            <PlaceholderView tabId={currentTab} />
          )}
        </main>
      </div>
    </div>
  );
}
