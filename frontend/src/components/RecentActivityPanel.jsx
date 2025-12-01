import React from 'react';
import {
  Paper,
  Typography,
  Box,
  List,
  ListItem,
  ListItemIcon,
  ListItemText,
  Divider,
} from '@mui/material';
import {
  Schedule as ScheduleIcon,
  CheckCircle,
  Error,
  Info,
} from '@mui/icons-material';

const RecentActivityPanel = ({ activities }) => {
  const formatDate = (dateString) => {
    if (!dateString) return 'Never';
    return new Date(dateString).toLocaleString();
  };

  const getActivityIcon = (type, status) => {
    if (status === 'success') return <CheckCircle sx={{ color: 'success.main' }} />;
    if (status === 'error') return <Error sx={{ color: 'error.main' }} />;
    return <Info sx={{ color: 'info.main' }} />;
  };

  return (
    <Paper 
      elevation={1} 
      sx={{ 
        pt: 3,
        px: 3,
        pb: 6,
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
        <ScheduleIcon sx={{ mr: 1.5, color: 'primary.main', fontSize: 24 }} />
        <Typography variant="h6" component="div" sx={{ fontWeight: 600 }}>
          Recent Activity
        </Typography>
      </Box>
      
      <Box sx={{ flex: 1, display: 'flex', flexDirection: 'column' }}>
        <Box sx={{ flex: 1, display: 'flex', alignItems: 'center', justifyContent: 'center' }}>
          {activities.length > 0 ? (
            <List sx={{ p: 0, width: '100%' }}>
              {activities.map((activity, index) => (
                <React.Fragment key={activity.id}>
                  <ListItem sx={{ px: 0, py: 1.5 }}>
                    <ListItemIcon sx={{ minWidth: 40 }}>
                      {getActivityIcon(activity.type, activity.status)}
                    </ListItemIcon>
                    <ListItemText
                      primary={
                        <Typography 
                          variant="body2" 
                          sx={{ 
                            fontWeight: 500,
                            color: 'text.primary'
                          }}
                        >
                          {activity.description}
                        </Typography>
                      }
                      secondary={
                        <Typography 
                          variant="caption" 
                          color="text.secondary"
                          sx={{ fontSize: '0.75rem' }}
                        >
                          {formatDate(activity.timestamp)}
                        </Typography>
                      }
                    />
                  </ListItem>
                  {index < activities.length - 1 && <Divider sx={{ mx: 0 }} />}
                </React.Fragment>
              ))}
            </List>
          ) : (
            <Box sx={{ textAlign: 'center', py: 4 }}>
              <ScheduleIcon sx={{ fontSize: 48, color: 'text.secondary', mb: 1, opacity: 0.5 }} />
              <Typography color="text.secondary" variant="body2">
                No recent activity
              </Typography>
            </Box>
          )}
        </Box>
        <Box sx={{ height: '60px' }} />
      </Box>
    </Paper>
  );
};

export default RecentActivityPanel;