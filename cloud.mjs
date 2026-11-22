export async function connect(config,handlers){
 const [A,U,F]=await Promise.all(['app','auth','firestore'].map(x=>import(`https://www.gstatic.com/firebasejs/13.1.0/firebase-${x}.js`)));
 const app=A.initializeApp(config.firebase),auth=U.getAuth(app),db=F.initializeFirestore(app,{localCache:F.memoryLocalCache()});
 const root=`clinics/${config.clinicId}`,ref=(...p)=>F.doc(db,root,...p),stateRef=ref('data','state');
 let user=null,member=null,offs=[],memberOff=null,dayOffs=[],generation=0;
 const clear=()=>{offs.forEach(f=>f());offs=[];dayOffs.forEach(f=>f());dayOffs=[];member=null;generation++;};
 const error=e=>handlers.error(e);
 U.onAuthStateChanged(auth,u=>{clear();memberOff?.();user=u;handlers.auth(u);if(!u)return;
  memberOff=F.onSnapshot(ref('members',u.uid),snap=>{const m=snap.exists()?snap.data():null;clear();member=m;if(!m?.active){handlers.denied(u);return;}handlers.member(m);offs.push(F.onSnapshot(stateRef,{includeMetadataChanges:true},s=>handlers.state(s.exists()?s.data():null,!s.metadata.fromCache),error));subscribeShared();},e=>{clear();handlers.denied(u);error(e);});
 });
 const sharedError=()=>handlers.error({code:'shared-denied',message:'Regole Firestore non aggiornate: pubblica il nuovo firestore.rules per usare note, assenze e contatore trattamenti.'});
 function subscribeShared(){const from=new Date(Date.now()-864e5).toISOString().slice(0,10),map=s=>Object.fromEntries(s.docs.map(x=>[x.id,x.data()]));
  offs.push(F.onSnapshot(F.query(F.collection(db,root,'leave'),F.where('date','>=',from)),s=>handlers.sync('leave',map(s)),sharedError));
  offs.push(F.onSnapshot(F.collection(db,root,'patientEdits'),s=>handlers.sync('edits',map(s)),sharedError));
  offs.push(F.onSnapshot(F.collection(db,root,'patientStats'),s=>handlers.sync('stats',map(s)),sharedError));}
 const allowed=()=>{if(!user||!member?.active)throw Error('Accesso non autorizzato.');};
 const coord=()=>{allowed();if(member.role!=='coordinator')throw Error('Operazione riservata al coordinatore.');};
 const audit=(tx,action)=>tx.set(F.doc(F.collection(db,root,'audit')),{action,actor:user.uid,at:F.serverTimestamp()});
 return {
  async login(email,password,remember){await U.setPersistence(auth,remember?U.browserLocalPersistence:U.browserSessionPersistence);return U.signInWithEmailAndPassword(auth,email,password);},reset:email=>U.sendPasswordResetEmail(auth,email),logout:()=>U.signOut(auth),
  async save(data,expected,action){coord();return F.runTransaction(db,async tx=>{const snap=await tx.get(stateRef),rev=snap.exists()?snap.data().revision:0;if(rev!==expected)throw Error('Un collega ha aggiornato i dati. Riapri la scheda prima di salvare.');tx.set(stateRef,{payload:data,revision:rev+1,updatedBy:user.uid,updatedAt:F.serverTimestamp()});audit(tx,action);});},
  subscribeDay(date,onPlan,onDone,onPrevious){allowed();dayOffs.forEach(f=>f());dayOffs=[];const my=++generation;
   dayOffs.push(F.onSnapshot(ref('plans',date),s=>{if(my===generation)onPlan(s.exists()?s.data():null);},error));
   dayOffs.push(F.onSnapshot(F.collection(db,root,'plans',date,'completions'),s=>{if(my===generation)onDone(Object.fromEntries(s.docs.map(x=>[x.id,x.data().done])));},error));
   dayOffs.push(F.onSnapshot(F.query(F.collection(db,root,'plans'),F.where('date','<',date),F.orderBy('date','desc'),F.limit(1)),s=>{if(my===generation)onPrevious(s.empty?null:s.docs[0].data());},error));
  },
  async finalize(date,rows,expected,assignments,expectedVersion=0){coord();const day=ref('plans',date);return F.runTransaction(db,async tx=>{const state=await tx.get(stateRef),old=await tx.get(day);if(!state.exists()||state.data().revision!==expected)throw Error('Dati cambiati: controlla il piano aggiornato e riprova.');const version=old.exists()?old.data().version:0;if(version!==expectedVersion)throw Error('Un collega ha pubblicato una nuova revisione. Controlla il piano aggiornato.');if(old.exists())tx.set(ref('plans',date,'versions',String(version)),old.data());tx.set(day,{date,version:version+1,rows,assignments,sourceRevision:expected,createdBy:user.uid,createdAt:F.serverTimestamp()});audit(tx,`Pubblicazione ${date} v${version+1}`);});},
  async done(date,pid,done,staff){allowed();const c=ref('plans',date,'completions',pid),st=ref('patientStats',pid);await F.runTransaction(db,async tx=>{const cs=await tx.get(c),ss=await tx.get(st),was=cs.exists()&&cs.data().done===true,delta=(done?1:0)-(was?1:0),d=ss.exists()?ss.data():{};tx.set(c,{done,updatedBy:user.uid,updatedAt:F.serverTimestamp()});if(delta){const by={...(d.treatedBy||{})};if(staff)by[staff]=Math.max(0,(by[staff]||0)+delta);tx.set(st,{count:Math.max(0,(d.count||0)+delta),last:done?date:(d.last||''),treatedBy:by,updatedBy:user.uid,updatedAt:F.serverTimestamp()},{merge:true});}});},
  async update(pid,fields,note,staff,date){allowed();const b=F.writeBatch(db),at=F.serverTimestamp();if(fields)b.set(ref('patientEdits',pid),{...fields,updatedBy:user.uid,updatedAt:at});if(note){b.set(F.doc(F.collection(db,root,'patientStats',pid,'notes')),{text:note,date,staffId:staff||'',by:user.uid,at});b.set(ref('patientStats',pid),{lastNote:note,lastNoteDate:date,lastNoteBy:staff||'',updatedBy:user.uid,updatedAt:at},{merge:true});}await b.commit();},
  async notes(pid){allowed();const s=await F.getDocs(F.query(F.collection(db,root,'patientStats',pid,'notes'),F.orderBy('at','desc'),F.limit(10)));return s.docs.map(x=>x.data());},
  async leave(date,staff,on){allowed();const r=ref('leave',`${date}_${staff}`);if(on)await F.setDoc(r,{date,staffId:staff,status:'Assente',by:user.uid,at:F.serverTimestamp()});else await F.deleteDoc(r);},
  async member(uid,data){coord();if(uid===user.uid)throw Error('Non puoi modificare il tuo accesso.');await F.setDoc(ref('members',uid),data);},
  async members(){coord();const s=await F.getDocs(F.collection(db,root,'members'));return s.docs.map(x=>({uid:x.id,...x.data()}));}
 };
}
