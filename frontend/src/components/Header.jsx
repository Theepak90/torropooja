import React from 'react';
import {
  AppBar,
  Toolbar,
  Typography,
  Box,
  Chip,
  IconButton,
} from '@mui/material';
import {
  CheckCircle,
  Refresh,
  Menu,
} from '@mui/icons-material';

const Header = ({ onRefresh, onMenuClick }) => {
  return (
      <AppBar 
        position="static" 
        elevation={0} 
        sx={{ 
          bgcolor: '#5C6BB5', 
          color: 'white',
          borderBottom: 'none',
          '& *': {
            fontFamily: 'Roboto, system-ui, Avenir, Helvetica, Arial, sans-serif !important'
          }
        }}
      >
      <Toolbar sx={{ px: 3 }}>
        <IconButton
          edge="start"
          color="inherit"
          onClick={onMenuClick}
          sx={{ 
            mr: 2,
            color: 'white',
            '&:hover': {
              backgroundColor: 'rgba(255, 255, 255, 0.1)'
            }
          }}
        >
          <Menu />
        </IconButton>
        
        {/* Logo and Title */}
        <Box sx={{ display: 'flex', alignItems: 'center', flexGrow: 1, gap: 2 }}>
          <img 
            src="/torro-logo.svg" 
            alt="Torro Logo" 
            style={{ 
              height: '40px', 
              width: '40px'
            }} 
          />
          <Box sx={{ display: 'flex', flexDirection: 'column' }}>
            <Typography 
              variant="h5" 
              component="div" 
              sx={{ 
                fontWeight: 'bold',
                fontSize: '1.5rem',
                color: 'white',
                lineHeight: 1.2,
                letterSpacing: '0.5px'
              }}
            >
              TORRO
            </Typography>
            <Typography 
              variant="caption" 
              sx={{ 
                color: 'rgba(255, 255, 255, 0.8)',
                fontSize: '0.7rem',
                letterSpacing: '1px'
              }}
            >
              Data Intelligence Platform
            </Typography>
          </Box>
        </Box>
        <Box sx={{ display: 'flex', alignItems: 'center', gap: 2 }}>
          <Chip
            icon={<CheckCircle sx={{ fontSize: 16 }} />}
            label="System Online"
            variant="outlined"
            size="small"
            sx={{ 
              fontWeight: 500,
              color: 'white',
              borderColor: 'white',
              '& .MuiChip-icon': {
                color: 'white'
              }
            }}
          />
          <IconButton 
            onClick={onRefresh} 
            size="small"
            sx={{ 
              color: 'white',
              '&:hover': {
                backgroundColor: 'rgba(255, 255, 255, 0.1)'
              }
            }}
          >
            <Refresh />
          </IconButton>
        </Box>
      </Toolbar>
    </AppBar>
  );
};

export default Header;
