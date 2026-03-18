// Safari-compatible anchor scrolling
(function(){
  // Handle initial hash on page load
  function scrollToHash(){
    var hash=window.location.hash;
    if(!hash)return;
    var target=document.querySelector(hash);
    if(target){
      setTimeout(function(){
        var y=target.getBoundingClientRect().top+window.pageYOffset-65;
        window.scrollTo({top:y,behavior:'smooth'});
      },100);
    }
  }

  // Handle clicks on anchor links
  document.addEventListener('click',function(e){
    var a=e.target.closest('a[href*="#"]');
    if(!a)return;
    var href=a.getAttribute('href');
    // Only handle same-page anchors
    var hashIndex=href.indexOf('#');
    if(hashIndex===-1)return;
    var page=href.substring(0,hashIndex);
    var hash=href.substring(hashIndex);
    // If it's a different page, let the browser handle it
    if(page&&!window.location.pathname.endsWith(page))return;
    var target=document.querySelector(hash);
    if(!target)return;
    e.preventDefault();
    history.pushState(null,null,hash);
    var y=target.getBoundingClientRect().top+window.pageYOffset-65;
    window.scrollTo({top:y,behavior:'smooth'});
  });

  // Handle popstate (browser back/forward)
  window.addEventListener('popstate',scrollToHash);

  // Handle initial load
  if(document.readyState==='complete'){scrollToHash()}
  else{window.addEventListener('load',scrollToHash)}

  // Sidebar active state tracking
  var headings=document.querySelectorAll('.doc-content h2[id], .doc-content h3[id]');
  var sidebarLinks=document.querySelectorAll('.sidebar a[href*="#"]');
  if(headings.length&&sidebarLinks.length){
    var observer=new IntersectionObserver(function(entries){
      entries.forEach(function(entry){
        if(entry.isIntersecting){
          sidebarLinks.forEach(function(l){l.classList.remove('active')});
          var match=document.querySelector('.sidebar a[href*="#'+entry.target.id+'"]');
          if(match)match.classList.add('active');
        }
      });
    },{rootMargin:'-70px 0px -70% 0px',threshold:0});
    headings.forEach(function(h){observer.observe(h)});
  }

  // Mobile sidebar toggle
  var toggle=document.querySelector('.sidebar-toggle');
  var sidebar=document.querySelector('.sidebar');
  if(toggle&&sidebar){
    toggle.addEventListener('click',function(){
      sidebar.classList.toggle('open');
      toggle.textContent=sidebar.classList.contains('open')?'\u2715':'\u2630';
    });
    // Close sidebar on link click (mobile)
    sidebar.addEventListener('click',function(e){
      if(e.target.tagName==='A'&&window.innerWidth<=900){
        sidebar.classList.remove('open');
        toggle.textContent='\u2630';
      }
    });
  }

  // Mobile nav burger
  window.closeMobileMenu=function(){
    var b=document.getElementById('burger');
    var l=document.getElementById('nav-links');
    if(b)b.classList.remove('open');
    if(l)l.classList.remove('open');
  };
})();
