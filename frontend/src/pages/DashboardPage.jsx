import React, { useState, useEffect } from 'react';
import {
  Box,
} from '@mui/material';

// Dashboard specific components
import SystemHealthPanel from '../components/SystemHealthPanel';
import RecentActivityPanel from '../components/RecentActivityPanel';
import DiscoveryStatisticsPanel from '../components/DiscoveryStatisticsPanel';

const DashboardPage = () => {
  const [stats, setStats] = useState({});
  const [systemHealth, setSystemHealth] = useState({});
  const [activities, setActivities] = useState([]);

  useEffect(() => {
    fetchDashboardData();
  }, []);

  const fetchDashboardData = async () => {
    try {
      // Create abort controllers for each request
      const statsController = new AbortController();
      const healthController = new AbortController();
      const activitiesController = new AbortController();
      
      // Set timeouts for each request
      const statsTimeout = setTimeout(() => statsController.abort(), 10000);
      const healthTimeout = setTimeout(() => healthController.abort(), 10000);
      const activitiesTimeout = setTimeout(() => activitiesController.abort(), 10000);

      const [statsRes, healthRes, activitiesRes] = await Promise.allSettled([
        fetch('http://localhost:8099/api/dashboard/stats', {
          signal: statsController.signal,
          headers: { 'Cache-Control': 'no-cache' }
        }),
        fetch('http://localhost:8099/api/system/health', {
          signal: healthController.signal,
          headers: { 'Cache-Control': 'no-cache' }
        }),
        fetch('http://localhost:8099/api/activities', {
          signal: activitiesController.signal,
          headers: { 'Cache-Control': 'no-cache' }
        }),
      ]);

      // Clear timeouts
      clearTimeout(statsTimeout);
      clearTimeout(healthTimeout);
      clearTimeout(activitiesTimeout);

      // Handle each response independently
      if (statsRes.status === 'fulfilled' && statsRes.value.ok) {
        const statsData = await statsRes.value.json();
        setStats(statsData);
      } else {
        console.warn('Stats request failed or timed out');
      }

      if (healthRes.status === 'fulfilled' && healthRes.value.ok) {
        const healthData = await healthRes.value.json();
        setSystemHealth(healthData);
      } else {
        console.warn('Health request failed or timed out');
      }

      if (activitiesRes.status === 'fulfilled' && activitiesRes.value.ok) {
        const activitiesData = await activitiesRes.value.json();
        setActivities(activitiesData);
      } else {
        console.warn('Activities request failed or timed out');
      }

    } catch (error) {
      console.error('Error fetching dashboard data:', error);
    }
  };

  return (
    <Box className="dashboard-container" sx={{ 
      width: '100%',
      display: 'grid',
      gridTemplateColumns: '1fr 1fr',
      gridTemplateRows: '240px 250px',
      gap: '24px',
      padding: '16px 24px',
      margin: 0
    }}>
      <Box sx={{ display: 'flex' }}>
        <SystemHealthPanel systemHealth={systemHealth} />
      </Box>
      <Box sx={{ display: 'flex' }}>
        <RecentActivityPanel activities={activities} />
      </Box>
      <Box sx={{ display: 'flex', gridColumn: '1 / -1' }}>
        <DiscoveryStatisticsPanel stats={stats} />
      </Box>
    </Box>
  );
};

export default DashboardPage;
