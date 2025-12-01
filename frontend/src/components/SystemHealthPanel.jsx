import React from 'react';
import {
  Paper,
  Typography,
  Box,
  Grid,
  Chip,
  Stack,
} from '@mui/material';
import {
  Shield,
  Visibility,
  Link,
  Schedule as ScheduleIcon,
  CheckCircle,
} from '@mui/icons-material';

const SystemHealthPanel = ({ systemHealth }) => {
  const formatDate = (dateString) => {
    if (!dateString) return 'Never';
    return new Date(dateString).toLocaleString();
  };

  return (
    <Paper 
      elevation={1} 
      sx={{ 
        pt: 3,
        px: 3,
        pb: 6,
        mb: 2,
        height: '240px',
        width: '100%',
        borderRadius: 0,
        display: 'flex',
        flexDirection: 'column',
        '&.MuiPaper-root': {
          paddingBottom: '48px !important'
        }
      }}
    >
      <Box sx={{ display: 'flex', alignItems: 'center', mb: 3 }}>
        <Shield sx={{ mr: 1.5, color: 'primary.main', fontSize: 24 }} />
        <Typography variant="h6" component="div" sx={{ fontWeight: 600 }}>
          System Health
        </Typography>
      </Box>
      
      <Box sx={{ flex: 1, display: 'flex', flexDirection: 'column' }}>
        <Grid container spacing={3}>
          <Grid item xs={6}>
            <Stack spacing={2}>
              <Box>
                <Typography 
                  variant="body2" 
                  color="text.secondary" 
                  gutterBottom
                  sx={{ fontWeight: 500 }}
                >
                  System Health Status
                </Typography>
                <Chip
                  icon={<CheckCircle sx={{ fontSize: 16 }} />}
                  label={systemHealth.status}
                  color="success"
                  size="small"
                  sx={{ fontWeight: 500 }}
                />
              </Box>
              <Box sx={{ mb: 4 }}>
                <Typography 
                  variant="body2" 
                  color="text.secondary" 
                  gutterBottom
                  sx={{ fontWeight: 500 }}
                >
                  Monitoring Status
                </Typography>
                <Chip
                  icon={<Visibility sx={{ fontSize: 16 }} />}
                  label={systemHealth.monitoring_enabled ? 'Enabled' : 'Disabled'}
                  color={systemHealth.monitoring_enabled ? 'success' : 'default'}
                  size="small"
                  sx={{ fontWeight: 500 }}
                />
              </Box>
            </Stack>
          </Grid>
          <Grid item xs={6}>
            <Stack spacing={2}>
              <Box>
                <Typography 
                  variant="body2" 
                  color="text.secondary" 
                  gutterBottom
                  sx={{ fontWeight: 500 }}
                >
                  Connectors
                </Typography>
                <Box sx={{ display: 'flex', alignItems: 'center' }}>
                  <Link sx={{ mr: 1, fontSize: 20, color: 'text.secondary' }} />
                  <Typography variant="body2" sx={{ fontWeight: 500 }}>
                    {systemHealth.connectors_enabled} of {systemHealth.connectors_total} enabled
                  </Typography>
                </Box>
              </Box>
              <Box sx={{ mb: 4 }}>
                <Typography 
                  variant="body2" 
                  color="text.secondary" 
                  gutterBottom
                  sx={{ fontWeight: 500 }}
                >
                  Last Scan
                </Typography>
                <Box sx={{ display: 'flex', alignItems: 'center' }}>
                  <ScheduleIcon sx={{ mr: 1, fontSize: 20, color: 'text.secondary' }} />
                  <Typography variant="body2" sx={{ fontWeight: 500 }}>
                    {formatDate(systemHealth.last_scan)}
                  </Typography>
                </Box>
              </Box>
            </Stack>
          </Grid>
        </Grid>
        <Box sx={{ height: '60px' }} />
      </Box>
    </Paper>
  );
};

export default SystemHealthPanel;