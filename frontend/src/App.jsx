import React, { useState, useMemo } from 'react';
import axios from 'axios';
import { Book, Music, Video, Upload, XCircle, CheckCircle } from 'lucide-react';

// Use environment variable for the API URL (falls back to localhost)
const API_URL = import.meta.env.VITE_API_URL || 'http://localhost:3000';
console.log("Current API URL:", API_URL);

function App() {
  const [category, setCategory] = useState('Book');
  const [file, setFile] = useState(null);
  const [uploading, setUploading] = useState(false);
  const [progress, setProgress] = useState(0);
  const [status, setStatus] = useState('idle'); // idle, uploading, success, error

  const [formData, setFormData] = useState({
    title: '',
    year: new Date().getFullYear().toString(),
    description: '',
    author: '',
    isbn: '',
    artist: '',
    resolution: ''
  });

  // Generate Year Dropdown options (last 50 years)
  const years = useMemo(() => {
    const currentYear = new Date().getFullYear();
    return Array.from({ length: 51 }, (_, i) => (currentYear - i).toString());
  }, []);

  const handleCategoryChange = (e) => {
    setCategory(e.target.value);
    // Reset specific fields to "forget" them when category changes
    setFormData(prev => ({ ...prev, author: '', isbn: '', artist: '', resolution: '' }));
  };

  const handleUpload = async () => {
    if (!file) return;

    setUploading(true);
    setStatus('uploading');
    
    // Prepare the metadata object
    const metadata = {
      category,
      title: formData.title,
      year: formData.year,
      description: formData.description,
      details: {
        author: formData.author,
        isbn: formData.isbn,
        artist: formData.artist,
        resolution: formData.resolution
      }
    };

    const data = new FormData();
    data.append('file', file);
    data.append('metadata', JSON.stringify(metadata));

    try {
      await axios.post(`${API_URL}/upload`, data, {
        onUploadProgress: (p) => {
          const percent = Math.round((p.loaded * 100) / p.total);
          setProgress(percent);
        }
      });
      setStatus('success');
    } catch (err) {
      console.error(err);
      setStatus('error');
    } finally {
      setUploading(false);
    }
  };

  return (
    <div style={{ maxWidth: '600px', margin: '40px auto', fontFamily: 'sans-serif', padding: '20px', border: '1px solid #ddd', borderRadius: '8px' }}>
      <h2 style={{ display: 'flex', alignItems: 'center', gap: '10px' }}>
        <Upload /> AWS Media Uploader
      </h2>

      <div style={{ marginBottom: '20px' }}>
        <label>Category: </label>
        <select value={category} onChange={handleCategoryChange} disabled={uploading}>
          <option value="Book">Book 📚</option>
          <option value="Audio">Audio 🎵</option>
          <option value="Video">Video 🎬</option>
        </select>
      </div>

      <div style={{ display: 'flex', flexDirection: 'column', gap: '10px' }}>
        <input placeholder="Item Title" value={formData.title} onChange={e => setFormData({...formData, title: e.target.value})} disabled={uploading} />
        
        <select value={formData.year} onChange={e => setFormData({...formData, year: e.target.value})} disabled={uploading}>
          {years.map(y => <option key={y} value={y}>{y}</option>)}
        </select>

        <textarea placeholder="Brief Description" onChange={e => setFormData({...formData, description: e.target.value})} disabled={uploading} />

        {/* Dynamic Fields */}
        {category === 'Book' && (
          <div style={{ display: 'flex', gap: '10px' }}>
            <input style={{ flex: 1 }} placeholder="Author Name" value={formData.author} onChange={e => setFormData({...formData, author: e.target.value})} disabled={uploading} />
            <input style={{ flex: 1 }} placeholder="ISBN" value={formData.isbn} onChange={e => setFormData({...formData, isbn: e.target.value})} disabled={uploading} />
          </div>
        )}
        
        {category === 'Audio' && <input placeholder="Artist / Band" value={formData.artist} onChange={e => setFormData({...formData, artist: e.target.value})} disabled={uploading} />}
        
        {category === 'Video' && <input placeholder="Resolution (e.g., 4K, 1080p)" value={formData.resolution} onChange={e => setFormData({...formData, resolution: e.target.value})} disabled={uploading} />}

        <input type="file" onChange={e => setFile(e.target.files[0])} disabled={uploading} style={{ marginTop: '10px' }} />
      </div>

      <div style={{ marginTop: '20px' }}>
        <button 
          onClick={handleUpload} 
          disabled={uploading || !file || !formData.title}
          style={{ width: '100%', padding: '10px', background: uploading ? '#ccc' : '#007bff', color: 'white', border: 'none', borderRadius: '4px', cursor: 'pointer' }}
        >
          {uploading ? 'Processing Multi-part Upload...' : 'Submit to AWS'}
        </button>
      </div>

      {/* Progress Bar & Status */}
      {status === 'uploading' && (
        <div style={{ marginTop: '20px' }}>
          <div style={{ height: '20px', width: '100%', background: '#eee', borderRadius: '10px', overflow: 'hidden' }}>
            <div style={{ height: '100%', width: `${progress}%`, background: '#28a745', transition: 'width 0.3s' }} />
          </div>
          <p style={{ textAlign: 'center', fontSize: '14px' }}>{progress}% Uploaded (Capturing logs in CloudWatch...)</p>
        </div>
      )}

      {status === 'success' && <p style={{ color: 'green', textAlign: 'center' }}><CheckCircle size={16} /> Upload Complete & Indexed!</p>}
      {status === 'error' && <p style={{ color: 'red', textAlign: 'center' }}><XCircle size={16} /> Error during upload. Check logs.</p>}
    </div>
  );
}

export default App;