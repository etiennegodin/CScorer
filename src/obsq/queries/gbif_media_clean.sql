


(
    length(g.mediaType) 
  - length(replace(g.mediaType, 'StillImage', ''))
) / length('StillImage') AS media_count