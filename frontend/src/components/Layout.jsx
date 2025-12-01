import React, { useState, useEffect } from 'react';
import { Outlet, useLocation } from 'react-router-dom';
import {
  Box,
} from '@mui/material';

// Components
import Header from './Header';
import SummaryCards from './SummaryCards';
import Sidebar from './Sidebar';

// Request deduplication cache
const requestCache = new Map();
const PENDING_REQUESTS = new Set();

const Layout = () => {
  const location = useLocation();
  const [sidebarOpen, setSidebarOpen] = useState(false);
  const [stats, setStats] = useState({
    total_assets: 0,
    total_catalogs: 0,
    active_connectors: 0,
    last_scan: null,
    monitoring_status: 'Unknown'
  });

  useEffect(() => {
    fetchDashboardData();
    // Refresh stats every 30 seconds
    const interval = setInterval(fetchDashboardData, 30000);
    return () => clearInterval(interval);
  }, []);

  const fetchDashboardData = async () => {
    const url = 'http://localhost:8099/api/dashboard/stats';
    
    // Check if request is already pending
    if (PENDING_REQUESTS.has(url)) {
      console.log('Stats request already pending, skipping duplicate');
      return;
    }
    
    // Check cache (5 second cache)
    const cacheKey = url;
    const cached = requestCache.get(cacheKey);
    if (cached && Date.now() - cached.timestamp < 5000) {
      console.log('Using cached stats data');
      setStats(cached.data);
      return;
    }
    
    try {
      PENDING_REQUESTS.add(url);
      
      // Add timeout and abort signal to prevent hanging requests
      const controller = new AbortController();
      const timeoutId = setTimeout(() => controller.abort(), 10000); // 10 second timeout
      
      const response = await fetch(url, {
        signal: controller.signal,
        headers: {
          'Cache-Control': 'no-cache',
          'Pragma': 'no-cache'
        }
      });
      
      clearTimeout(timeoutId);
      
      if (!response.ok) {
        throw new Error(`HTTP ${response.status}: ${response.statusText}`);
      }
      
      const data = await response.json();
      
      // Cache the result
      requestCache.set(cacheKey, {
        data,
        timestamp: Date.now()
      });
      
      setStats(data);
    } catch (error) {
      if (error.name === 'AbortError') {
        console.warn('Dashboard stats request timed out after 10 seconds');
      } else {
        console.error('Error fetching dashboard data:', error);
      }
      // Don't update state on error to avoid clearing existing data
    } finally {
      PENDING_REQUESTS.delete(url);
    }
  };

  const handleMenuClick = () => {
    setSidebarOpen(true);
  };

  const handleSidebarClose = () => {
    setSidebarOpen(false);
  };

  // Only show summary cards on dashboard page
  const showSummaryCards = location.pathname === '/';

  return (
    <Box sx={{ 
      flexGrow: 1, 
      minHeight: '100vh', 
      backgroundColor: 'background.default',
      width: '100%',
      maxWidth: 'none'
    }}>
      <Header onRefresh={fetchDashboardData} onMenuClick={handleMenuClick} />
      {showSummaryCards && <SummaryCards stats={stats} />}
      <Sidebar open={sidebarOpen} onClose={handleSidebarClose} />
      <Outlet />
    </Box>
  );
};

export default Layout;
