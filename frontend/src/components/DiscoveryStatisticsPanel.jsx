import React from 'react';
import {
  Paper,
  Typography,
  Box,
  Grid,
  Card,
  CardContent,
  LinearProgress,
} from '@mui/material';
import {
  Assessment,
  TrendingUp,
  DataObject,
  Speed,
} from '@mui/icons-material';

const DiscoveryStatisticsPanel = ({ stats = [] }) => {

  return (
    <Paper 
      elevation={1} 
      sx={{ 
        p: 3,
        height: '250px',
        width: '100%',
        borderRadius: 0,
      }}
    >
      <Box sx={{ display: 'flex', alignItems: 'center', mb: 3 }}>
        <Assessment sx={{ mr: 1.5, color: 'primary.main', fontSize: 24 }} />
        <Typography variant="h6" component="div" sx={{ fontWeight: 600 }}>
          Discovery Statistics
        </Typography>
      </Box>
      
      {stats.length > 0 ? (
        <Grid container spacing={3}>
          {stats.map((stat, index) => (
            <Grid item xs={12} sm={6} md={3} key={index}>
              <Card 
                elevation={0}
                sx={{ 
                  height: '100%',
                  border: '1px solid #e5e7eb',
                  borderRadius: 2,
                }}
              >
                <CardContent sx={{ p: 2.5 }}>
                  <Box sx={{ display: 'flex', alignItems: 'center', mb: 2 }}>
                    {stat.icon}
                    <Typography 
                      variant="body2" 
                      color="text.secondary" 
                      sx={{ ml: 1, fontWeight: 500 }}
                    >
                      {stat.title}
                    </Typography>
                  </Box>
                  <Typography 
                    variant="h5" 
                    component="div" 
                    sx={{ 
                      fontWeight: 'bold',
                      color: 'text.primary',
                      mb: 1
                    }}
                  >
                    {stat.value}
                  </Typography>
                  <LinearProgress
                    variant="determinate"
                    value={stat.progress}
                    sx={{
                      height: 6,
                      borderRadius: 3,
                      backgroundColor: 'grey.200',
                      '& .MuiLinearProgress-bar': {
                        borderRadius: 3,
                        backgroundColor: `${stat.color}.main`,
                      },
                    }}
                  />
                  <Typography 
                    variant="caption" 
                    color="text.secondary"
                    sx={{ mt: 1, display: 'block' }}
                  >
                    {stat.progress}% complete
                  </Typography>
                </CardContent>
              </Card>
            </Grid>
          ))}
        </Grid>
      ) : (
        <Box sx={{ textAlign: 'center', py: 4 }}>
          <Assessment sx={{ fontSize: 48, color: 'text.secondary', mb: 1, opacity: 0.5 }} />
          <Typography color="text.secondary" variant="body2">
            No discovery statistics available
          </Typography>
        </Box>
      )}
    </Paper>
  );
};

export default DiscoveryStatisticsPanel;
