import React from 'react';
import {
  Box,
  Card,
  CardContent,
  Typography,
} from '@mui/material';
import {
  DataObject,
  CloudSync,
  Schedule,
} from '@mui/icons-material';

const SummaryCards = ({ stats }) => {
  const formatDate = (dateString) => {
    if (!dateString) return 'Never';
    return new Date(dateString).toLocaleString();
  };

  const cards = [
    {
      title: 'Total Assets',
      value: stats.total_assets || 0,
      subtitle: 'Assets Found',
      icon: <DataObject sx={{ fontSize: 24, color: 'primary.main' }} />,
      color: 'primary',
    },
    {
      title: 'Active Connectors',
      value: stats.active_connectors,
      subtitle: 'Connected',
      icon: <CloudSync sx={{ fontSize: 24, color: 'success.main' }} />,
      color: 'success',
    },
    {
      title: 'Last Scan',
      value: formatDate(stats.last_scan),
      subtitle: 'System Scan',
      icon: <Schedule sx={{ fontSize: 24, color: 'info.main' }} />,
      color: 'info',
    },
  ];

  return (
    <Box className="dashboard-container" sx={{ 
      width: '100%',
      maxWidth: 'none',
      display: 'grid',
      gridTemplateColumns: 'repeat(3, 1fr)',
      gap: '16px',
      padding: '16px 24px',
      margin: 0
    }}>
      {cards.map((card, index) => (
        <Card 
              key={index}
              elevation={1}
              sx={{ 
                height: '140px',
                width: '100%',
                borderRadius: 0,
                display: 'flex',
                flexDirection: 'column',
                transition: 'all 0.2s ease-in-out',
                '&:hover': {
                  transform: 'translateY(-2px)',
                  boxShadow: '0 4px 12px 0 rgba(0, 0, 0, 0.15)',
                }
              }}
            >
              <CardContent sx={{ 
                p: 3, 
                display: 'flex', 
                flexDirection: 'column', 
                height: '100%',
                justifyContent: 'space-between'
              }}>
                <Box sx={{ display: 'flex', alignItems: 'center', mb: 2 }}>
                  {card.icon}
                  <Typography 
                    variant="body2" 
                    color="text.secondary" 
                    sx={{ ml: 1, fontWeight: 500 }}
                  >
                    {card.title}
                  </Typography>
                </Box>
                <Box sx={{ flex: 1, display: 'flex', flexDirection: 'column', justifyContent: 'center' }}>
                  <Typography 
                    variant="h4" 
                    component="div" 
                    sx={{ 
                      fontWeight: 'bold',
                      color: 'text.primary',
                      mb: 0.5,
                      fontSize: '1.75rem'
                    }}
                  >
                    {card.value}
                  </Typography>
                  <Typography 
                    variant="body2" 
                    color="text.secondary"
                    sx={{ fontSize: '0.875rem' }}
                  >
                    {card.subtitle}
                  </Typography>
                </Box>
              </CardContent>
            </Card>
      ))}
    </Box>
  );
};

export default SummaryCards;
