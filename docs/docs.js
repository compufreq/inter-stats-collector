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

  // --- Search functionality ---
  (function(){
    var searchIndex=null;
    var modal=null;
    var input=null;
    var resultsDiv=null;

    function el(tag,attrs,children){
      var e=document.createElement(tag);
      if(attrs)Object.keys(attrs).forEach(function(k){
        if(k==='text')e.textContent=attrs[k];
        else if(k==='class')e.className=attrs[k];
        else if(k.indexOf('on')===0)e.addEventListener(k.slice(2),attrs[k]);
        else e.setAttribute(k,attrs[k]);
      });
      if(children)children.forEach(function(c){if(c)e.appendChild(typeof c==='string'?document.createTextNode(c):c)});
      return e;
    }

    function buildModal(){
      var searchIcon=document.createElementNS('http://www.w3.org/2000/svg','svg');
      searchIcon.setAttribute('viewBox','0 0 24 24');
      var c=document.createElementNS('http://www.w3.org/2000/svg','circle');
      c.setAttribute('cx','11');c.setAttribute('cy','11');c.setAttribute('r','8');
      var l=document.createElementNS('http://www.w3.org/2000/svg','line');
      l.setAttribute('x1','21');l.setAttribute('y1','21');l.setAttribute('x2','16.65');l.setAttribute('y2','16.65');
      searchIcon.appendChild(c);searchIcon.appendChild(l);

      input=el('input',{'class':'search-input',type:'text',placeholder:'Search documentation...',autocomplete:'off'});
      var closeBtn=el('button',{'class':'search-close',text:'ESC',onclick:closeSearch});
      var inputWrap=el('div',{'class':'search-input-wrap'},[searchIcon,input,closeBtn]);
      resultsDiv=el('div',{'class':'search-results'},[el('div',{'class':'search-empty',text:'Type to search across all documentation pages'})]);
      var hint=el('div',{'class':'search-hint',text:'Navigate with \u2191\u2193 \u00b7 Open with Enter \u00b7 Close with Esc'});
      var box=el('div',{'class':'search-box-wrap'},[inputWrap,resultsDiv,hint]);
      modal=el('div',{'class':'search-modal',onclick:function(e){if(e.target===modal)closeSearch()}},[box]);
      document.body.appendChild(modal);

      input.addEventListener('input',function(){doSearch(input.value)});
      input.addEventListener('keydown',function(e){
        var items=resultsDiv.querySelectorAll('.search-result');
        var active=resultsDiv.querySelector('.search-result:focus');
        if(e.key==='ArrowDown'){
          e.preventDefault();
          if(!active&&items.length)items[0].focus();
          else if(active&&active.nextElementSibling&&active.nextElementSibling.classList.contains('search-result'))active.nextElementSibling.focus();
        }else if(e.key==='ArrowUp'){
          e.preventDefault();
          if(active&&active.previousElementSibling&&active.previousElementSibling.classList.contains('search-result'))active.previousElementSibling.focus();
          else if(active)input.focus();
        }else if(e.key==='Enter'){
          e.preventDefault();
          var focused=resultsDiv.querySelector('.search-result:focus')||items[0];
          if(focused)window.location.href=focused.getAttribute('href');
        }else if(e.key==='Escape'){closeSearch()}
      });
    }

    function openSearch(){
      if(!modal)buildModal();
      modal.classList.add('open');
      input.value='';
      while(resultsDiv.firstChild)resultsDiv.removeChild(resultsDiv.firstChild);
      resultsDiv.appendChild(el('div',{'class':'search-empty',text:'Type to search across all documentation pages'}));
      setTimeout(function(){input.focus()},50);
      if(!searchIndex)loadIndex();
    }

    function closeSearch(){if(modal)modal.classList.remove('open')}

    function loadIndex(){
      fetch('search-index.json').then(function(r){return r.json()}).then(function(data){
        searchIndex=data;
      }).catch(function(){
        searchIndex=[];
        while(resultsDiv.firstChild)resultsDiv.removeChild(resultsDiv.firstChild);
        resultsDiv.appendChild(el('div',{'class':'search-empty',text:'Search index not available'}));
      });
    }

    function escHtml(s){return s.replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;')}

    function highlightInto(parent,text,terms){
      // Build text with <mark> highlights using DOM methods
      var lower=text.toLowerCase();
      var positions=[];
      terms.forEach(function(term){
        var idx=0;
        while((idx=lower.indexOf(term,idx))!==-1){
          positions.push({start:idx,end:idx+term.length});
          idx+=term.length;
        }
      });
      // Sort and merge overlapping
      positions.sort(function(a,b){return a.start-b.start});
      var merged=[];
      positions.forEach(function(p){
        if(merged.length&&p.start<=merged[merged.length-1].end){
          merged[merged.length-1].end=Math.max(merged[merged.length-1].end,p.end);
        }else{merged.push({start:p.start,end:p.end})}
      });
      var last=0;
      merged.forEach(function(p){
        if(p.start>last)parent.appendChild(document.createTextNode(text.substring(last,p.start)));
        var mark=document.createElement('mark');
        mark.textContent=text.substring(p.start,p.end);
        parent.appendChild(mark);
        last=p.end;
      });
      if(last<text.length)parent.appendChild(document.createTextNode(text.substring(last)));
    }

    function doSearch(query){
      while(resultsDiv.firstChild)resultsDiv.removeChild(resultsDiv.firstChild);
      if(!searchIndex||!query.trim()){
        resultsDiv.appendChild(el('div',{'class':'search-empty',text:query.trim()?'Loading...':'Type to search across all documentation pages'}));
        return;
      }
      var q=query.toLowerCase().trim();
      var terms=q.split(/\s+/).filter(function(t){return t.length>0});
      var scored=[];

      searchIndex.forEach(function(section){
        var text=(section.heading+' '+section.text+' '+section.pageTitle).toLowerCase();
        var score=0;var allMatch=true;
        terms.forEach(function(term){
          if(text.indexOf(term)===-1){allMatch=false}
          else{
            if(section.heading.toLowerCase().indexOf(term)!==-1)score+=10;
            var idx=0;var count=0;
            while((idx=text.indexOf(term,idx))!==-1){count++;idx+=term.length}
            score+=count;
          }
        });
        if(allMatch&&score>0)scored.push({section:section,score:score});
      });

      scored.sort(function(a,b){return b.score-a.score});

      if(scored.length===0){
        resultsDiv.appendChild(el('div',{'class':'search-empty',text:'No results for "'+query.trim()+'"'}));
        return;
      }

      scored.slice(0,20).forEach(function(item){
        var s=item.section;
        var url=s.page+(s.anchor?'#'+s.anchor:'');
        var a=el('a',{'class':'search-result',href:url,tabindex:'0'});
        a.appendChild(el('div',{'class':'sr-page',text:s.pageTitle}));
        var headingDiv=el('div',{'class':'sr-heading'});
        highlightInto(headingDiv,s.heading,terms);
        a.appendChild(headingDiv);
        // Snippet
        var lower=s.text.toLowerCase();
        var pos=-1;
        for(var i=0;i<terms.length;i++){pos=lower.indexOf(terms[i]);if(pos!==-1)break}
        if(pos===-1)pos=0;
        var start=Math.max(0,pos-60);
        var end=Math.min(s.text.length,pos+200);
        var snippet=(start>0?'...':'')+s.text.substring(start,end)+(end<s.text.length?'...':'');
        var textDiv=el('div',{'class':'sr-text'});
        highlightInto(textDiv,snippet,terms);
        a.appendChild(textDiv);
        resultsDiv.appendChild(a);
      });
    }

    // Global keyboard shortcut: Ctrl/Cmd + K or /
    document.addEventListener('keydown',function(e){
      if((e.ctrlKey||e.metaKey)&&e.key==='k'){e.preventDefault();openSearch()}
      if(e.key==='/'&&!['INPUT','TEXTAREA','SELECT'].includes(document.activeElement.tagName)){e.preventDefault();openSearch()}
      if(e.key==='Escape')closeSearch();
    });

    // Add search trigger button to nav
    var navLinks=document.getElementById('nav-links');
    if(navLinks){
      var svgNS='http://www.w3.org/2000/svg';
      var icon=document.createElementNS(svgNS,'svg');
      icon.setAttribute('width','14');icon.setAttribute('height','14');icon.setAttribute('viewBox','0 0 24 24');
      icon.setAttribute('fill','none');icon.setAttribute('stroke','currentColor');icon.setAttribute('stroke-width','2');
      var ci=document.createElementNS(svgNS,'circle');ci.setAttribute('cx','11');ci.setAttribute('cy','11');ci.setAttribute('r','8');
      var li=document.createElementNS(svgNS,'line');li.setAttribute('x1','21');li.setAttribute('y1','21');li.setAttribute('x2','16.65');li.setAttribute('y2','16.65');
      icon.appendChild(ci);icon.appendChild(li);
      var kbd=el('kbd',{text:'/'});
      var btn=el('button',{'class':'search-trigger',onclick:openSearch},[icon,document.createTextNode('Search '),kbd]);
      var ghBtn=navLinks.querySelector('.gh-btn');
      if(ghBtn)navLinks.insertBefore(btn,ghBtn);
      else navLinks.appendChild(btn);
    }
    // Mobile search FAB
    var fabSvg=document.createElementNS(svgNS,'svg');
    fabSvg.setAttribute('viewBox','0 0 24 24');
    var fabC=document.createElementNS(svgNS,'circle');fabC.setAttribute('cx','11');fabC.setAttribute('cy','11');fabC.setAttribute('r','8');
    var fabL=document.createElementNS(svgNS,'line');fabL.setAttribute('x1','21');fabL.setAttribute('y1','21');fabL.setAttribute('x2','16.65');fabL.setAttribute('y2','16.65');
    fabSvg.appendChild(fabC);fabSvg.appendChild(fabL);
    var fab=el('button',{'class':'search-fab','aria-label':'Search',onclick:openSearch},[fabSvg]);
    document.body.appendChild(fab);
  })();

  // Highlight current page in nav + dropdown
  (function(){
    var path=window.location.pathname.split('/').pop()||'index.html';
    // Mark matching links in desktop nav and dropdown
    document.querySelectorAll('nav .links a, nav .dropdown-menu a').forEach(function(a){
      var href=(a.getAttribute('href')||'').split('/').pop().split('#')[0]||'index.html';
      if(href===path){
        a.style.color='#3b82f6';
      }
    });
  })();

  // Mobile nav — fullscreen overlay menu
  (function(){
    var links=document.getElementById('nav-links');
    var burger=document.getElementById('burger');
    if(!links||!burger)return;

    // Build overlay with inline styles to avoid CSS context issues
    var overlay=document.createElement('div');
    overlay.id='mobile-overlay';
    overlay.setAttribute('style','display:none;position:fixed;top:0;left:0;right:0;bottom:0;width:100vw;height:100vh;background:#0a0e17;z-index:9999;flex-direction:column;align-items:center;justify-content:center;gap:2rem;');

    var currentPage=window.location.pathname.split('/').pop()||'index.html';
    function isActive(href){return(href||'').split('/').pop().split('#')[0]===currentPage||((href||'')==='./'+currentPage)}

    Array.from(links.children).forEach(function(child){
      if(child.classList&&child.classList.contains('dropdown')){
        // Flatten dropdown: add each menu link directly
        var menu=child.querySelector('.dropdown-menu');
        if(menu){
          Array.from(menu.children).forEach(function(item){
            if(item.tagName==='A'){
              var clone=item.cloneNode(true);
              var active=isActive(clone.getAttribute('href'));
              clone.setAttribute('style','font-size:1.1rem;color:'+(active?'#3b82f6':'#94a3b8')+';text-decoration:none;'+(active?'font-weight:600;':''));
              clone.addEventListener('click',function(){closeMobileMenu()});
              overlay.appendChild(clone);
            }
          });
        }
      } else if(child.tagName==='A'){
        var clone=child.cloneNode(true);
        var active=isActive(clone.getAttribute('href'));
        clone.setAttribute('style','font-size:1.3rem;color:'+(active?'#3b82f6':'#e2e8f0')+';text-decoration:none;'+(active?'font-weight:600;':''));
        clone.addEventListener('click',function(){closeMobileMenu()});
        overlay.appendChild(clone);
      }
    });
    document.body.appendChild(overlay);

    burger.addEventListener('click',function(){
      var isOpen=overlay.style.display==='flex';
      overlay.style.display=isOpen?'none':'flex';
      burger.classList.toggle('open',!isOpen);
    });
  })();

  window.closeMobileMenu=function(){
    var b=document.getElementById('burger');
    var o=document.getElementById('mobile-overlay');
    if(b)b.classList.remove('open');
    if(o)o.style.display='none';
  };
})();
