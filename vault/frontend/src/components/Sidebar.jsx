import React from 'react';
import {
  LayoutDashboard,
  HardDrive,
  Database,
  Copy,
  Wrench,
  ShieldCheck,
  Flame,
  Activity,
  Layers,
  Scale,
} from 'lucide-react';

const NAV_GROUPS = [
  {
    title: 'Platform Overview',
    items: [
      { id: 'dashboard', label: 'Cluster Dashboard', icon: LayoutDashboard },
      { id: 'nodes', label: 'Storage Nodes', icon: HardDrive, hasBadge: true },
      { id: 'events', label: 'Live Events', icon: Activity, badgeText: 'WS' },
    ],
  },
  {
    title: 'Object & Data Mesh',
    items: [
      { id: 'objects', label: 'Object Catalog', icon: Database },
      { id: 'replication', label: 'Replication Mesh', icon: Copy },
      { id: 'repair', label: 'Repair Center', icon: Wrench },
      { id: 'integrity', label: 'Data Integrity', icon: ShieldCheck },
      { id: 'rebalancing', label: 'Rebalancing', icon: Scale },
    ],
  },
  {
    title: 'Resilience & Simulation',
    items: [
      { id: 'fault_lab', label: 'Fault Injection', icon: Flame },
    ],
  },
];

export default function Sidebar({ currentTab, onSelectTab, nodeCount = 5, healthyNodeCount = 5 }) {
  return (
    <aside className="sidebar" aria-label="Sidebar">
      {/* Brand Header */}
      <div className="sidebar-brand">
        <div className="vault-logo-glyph">
          <Layers size={22} strokeWidth={2.4} aria-hidden="true" />
        </div>
        <div>
          <div className="vault-brand-title">
            <span>Vault</span>
            <span
              style={{
                fontSize: '0.62rem',
                color: '#2563EB',
                background: '#EFF6FF',
                border: '1px solid #DBEAFE',
                padding: '2px 6px',
                borderRadius: '4px',
                fontWeight: 700,
                letterSpacing: '0.04em',
              }}
            >
              DISTRIBUTED
            </span>
          </div>
          <div className="vault-brand-subtitle">
            Enterprise Object Storage
          </div>
        </div>
      </div>

      {/* Navigation Sections */}
      <nav className="sidebar-nav" aria-label="Primary navigation">
        {NAV_GROUPS.map((group, gIdx) => (
          <div key={gIdx} style={{ marginBottom: '8px' }}>
            <div className="nav-section-label">{group.title}</div>
            {group.items.map((item) => {
              const Icon = item.icon;
              const isActive = currentTab === item.id;
              return (
                <button
                  key={item.id}
                  onClick={() => onSelectTab(item.id)}
                  className={`nav-item ${isActive ? 'active' : ''}`}
                  aria-current={isActive ? 'page' : undefined}
                >
                  <Icon size={18} strokeWidth={isActive ? 2.2 : 1.8} className="nav-icon" aria-hidden="true" />
                  <span>{item.label}</span>

                  {item.hasBadge && (
                    <span className={`nav-badge-pill ${healthyNodeCount === nodeCount ? 'live' : ''}`}>
                      {healthyNodeCount}/{nodeCount}
                    </span>
                  )}

                  {item.badgeText && (
                    <span className="nav-badge-pill primary">
                      {item.badgeText}
                    </span>
                  )}
                </button>
              );
            })}
          </div>
        ))}
      </nav>

      {/* Footer System Specs */}
      <div className="sidebar-footer">
        <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
          <span style={{ color: '#475569' }}>TOPOLOGY</span>
          <span style={{ fontWeight: 600, color: '#172033' }}>5 Nodes + Coord</span>
        </div>
        <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
          <span style={{ color: '#475569' }}>POLICY</span>
          <span style={{ fontWeight: 600, color: '#2563EB' }}>RF=3 · W=2 (HRW)</span>
        </div>
        <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
          <span style={{ color: '#475569' }}>INTEGRITY</span>
          <span style={{ fontWeight: 600, color: '#10B981' }}>SHA-256 Verifier</span>
        </div>
      </div>
    </aside>
  );
}
